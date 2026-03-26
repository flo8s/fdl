"""GC: clean up orphaned Parquet files on R2."""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from fdl import DUCKLAKE_FILE, ducklake_data_path
from fdl.config import datasource_name, s3_access_key_id, s3_endpoint, s3_secret_access_key
from fdl.s3 import create_s3_client


def _format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _count_scheduled_files(ducklake_file: Path) -> int:
    """Count files in ducklake_files_scheduled_for_deletion."""
    conn = duckdb.connect(str(ducklake_file), read_only=True)
    try:
        result = conn.execute(
            "SELECT count(*) FROM ducklake_files_scheduled_for_deletion"
        ).fetchone()
        return result[0] if result else 0
    finally:
        conn.close()


def _cleanup_scheduled_files(
    ducklake_file: Path, datasource: str, bucket: str
) -> int:
    """Run ducklake_cleanup_old_files to delete scheduled files from R2."""
    data_path = ducklake_data_path(f"s3://{bucket}/{datasource}/{DUCKLAKE_FILE}")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"""
            SET s3_endpoint = '{s3_endpoint().removeprefix("https://")}';
            SET s3_access_key_id = '{s3_access_key_id()}';
            SET s3_secret_access_key = '{s3_secret_access_key()}';
            SET s3_region = 'auto';
        """)
        conn.execute(f"""
            ATTACH '{ducklake_file}' AS {datasource} (
                TYPE ducklake,
                DATA_PATH '{data_path}',
                OVERRIDE_DATA_PATH true
            )
        """)
        result = conn.execute(
            f"CALL ducklake_cleanup_old_files('{datasource}')"
        ).fetchone()
        return result[0] if result else 0
    finally:
        conn.close()


def _get_active_files(ducklake_file: Path) -> set[str]:
    """Get set of active file paths from DuckLake metadata."""
    conn = duckdb.connect(str(ducklake_file), read_only=True)
    try:
        rows = conn.execute("""
            SELECT DISTINCT s.path || t.path || f.path AS full_path
            FROM ducklake_data_file f
            JOIN ducklake_table t ON f.table_id = t.table_id
            JOIN ducklake_schema s ON t.schema_id = s.schema_id
            WHERE f.end_snapshot IS NULL
        """).fetchall()
        return {row[0] for row in rows}
    finally:
        conn.close()


def _list_r2_files(client, bucket: str, prefix: str) -> dict[str, dict]:
    """List all files on R2 under the given prefix."""
    r2_files = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            relative_path = obj["Key"][len(prefix):]
            r2_files[relative_path] = obj
    return r2_files


def gc_datasource(
    dataset_dir: Path,
    dist_dir: Path,
    *,
    bucket: str,
    force: bool = False,
    older_than_days: int | None = None,
) -> None:
    """Clean up orphaned Parquet files for a datasource."""
    datasource = datasource_name(dataset_dir)
    print(f"--- gc: {datasource} ---")

    ducklake_file = dist_dir / DUCKLAKE_FILE
    if not ducklake_file.exists():
        raise FileNotFoundError(
            f"{ducklake_file} not found. Run 'fdl pull' first."
        )

    # Step 1: Handle files scheduled for deletion by DuckLake
    scheduled = _count_scheduled_files(ducklake_file)
    print(
        f"[Step 1] ducklake_cleanup_old_files: {scheduled} files scheduled for deletion"
    )

    # Step 2: Find orphaned files on R2
    active_files = _get_active_files(ducklake_file)

    client = create_s3_client()
    prefix = f"{datasource}/{ducklake_data_path(DUCKLAKE_FILE)}"
    r2_files = _list_r2_files(client, bucket, prefix)

    orphaned = {}
    for rel_path, obj in r2_files.items():
        if rel_path in active_files:
            continue
        if older_than_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
            if obj["LastModified"] > cutoff:
                continue
        orphaned[rel_path] = obj

    orphaned_size = sum(obj["Size"] for obj in orphaned.values())
    print("[Step 2] Orphaned files on R2:")
    print(f"  Active: {len(active_files)} files")
    print(f"  R2 total: {len(r2_files)} files")
    print(f"  Orphaned: {len(orphaned)} files ({_format_size(orphaned_size)})")

    if not orphaned:
        return

    for rel_path in sorted(orphaned):
        print(f"  {prefix}{rel_path}")

    # Confirm deletion
    if not force:
        answer = input(f"\nDelete {len(orphaned)} files? [y/N] ")
        if answer.lower() not in ("y", "yes"):
            print("Aborted.")
            return

    # Step 1 actual: Run ducklake_cleanup_old_files
    if scheduled > 0:
        cleanup_count = _cleanup_scheduled_files(ducklake_file, datasource, bucket)
        print(f"\nducklake_cleanup_old_files: {cleanup_count} files deleted")

    # Delete orphaned files in batches (S3 limit: 1000 per request)
    print(f"Deleting {len(orphaned)} orphaned files...")
    keys = [{"Key": f"{prefix}{p}"} for p in orphaned]
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    print(f"Deleted {len(orphaned)} files ({_format_size(orphaned_size)}).")
