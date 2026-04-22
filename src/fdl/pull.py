"""Pull: download DuckLake catalog from S3 or local directory."""

from __future__ import annotations

import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, META_JSON
from fdl.console import console


def do_pull(
    resolved: str,
    target: str,
    dist_dir: Path,
    datasource: str,
    project_dir: Path | None = None,
) -> None:
    """Pull catalog from a target (S3 or local)."""
    if resolved.startswith("s3://"):
        from fdl.config import target_s3_config
        from fdl.s3 import create_s3_client

        s3 = target_s3_config(target, project_dir)
        client = create_s3_client(s3)
        fetch_from_s3(
            client, s3.bucket, dist_dir, datasource,
            target_name=target, project_dir=project_dir,
        )
    else:
        pull_from_local(Path(resolved), dist_dir, datasource)


def pull_if_needed(
    target_dir: Path,
    resolved: str,
    target: str,
    datasource: str,
    project_dir: Path | None = None,
) -> str | None:
    """Pull if local catalog is missing, unsynced, or stale.

    Returns the reason for pulling, or None if already up to date.

    - Local targets: pull only when the catalog file is missing (no
      ETag-based stale detection).
    - S3 targets: compare the saved remote ETag with the server's
      current ETag via HEAD and pull on mismatch.
    """
    catalog_exists = (
        (target_dir / DUCKLAKE_FILE).exists()
        or (target_dir / DUCKLAKE_SQLITE).exists()
    )
    if not catalog_exists:
        reason = "No local catalog"
    elif resolved.startswith("s3://"):
        reason = _s3_stale_reason(target_dir, resolved, target, datasource, project_dir)
        if reason is None:
            return None
    else:
        return None

    do_pull(resolved, target, target_dir, datasource, project_dir)
    return reason


def _s3_stale_reason(
    target_dir: Path,
    resolved: str,
    target: str,
    datasource: str,
    project_dir: Path | None,
) -> str | None:
    """Return a reason string when the S3 remote has diverged from local state."""
    from fdl.config import target_s3_config
    from fdl.meta import read_remote_etag
    from fdl.s3 import create_s3_client

    local_etag = read_remote_etag(target_dir / META_JSON)
    if local_etag is None:
        return "Catalog not synced"

    s3 = target_s3_config(target, project_dir)
    remote_etag = _head_catalog_etag(
        create_s3_client(s3), s3.bucket, f"{datasource}/{DUCKLAKE_FILE}"
    )
    if remote_etag is None:
        return None
    if remote_etag != local_etag:
        return "Remote is newer"
    return None


def pull_from_local(
    source_dir: Path,
    dist_dir: Path,
    datasource: str,
) -> bool:
    """Copy catalog from a local directory into dist/.

    Returns True if catalog was found. Local targets do not maintain
    conflict-detection state.
    """
    src = source_dir / datasource
    if not src.exists():
        return False

    dist_dir.mkdir(parents=True, exist_ok=True)

    src_file = src / DUCKLAKE_FILE
    if src_file.exists():
        console.print(f"  [dim]{datasource}/{DUCKLAKE_FILE}[/dim]")
        shutil.copy2(src_file, dist_dir / DUCKLAKE_FILE)

    return True


def _download_file(client, bucket: str, key: str, dest: Path) -> bool:
    """Download a single file. Returns True if successful, False if 404."""
    from botocore.exceptions import ClientError

    try:
        console.print(f"  [dim]{key}[/dim]")
        client.download_file(bucket, key, str(dest))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            console.print(f"  [yellow]{key} not found, skipping[/yellow]")
            return False
        raise


def _head_catalog_etag(client, bucket: str, key: str) -> str | None:
    """Return the current ETag of the remote catalog, or None if absent."""
    from botocore.exceptions import ClientError

    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response["Error"].get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey"} or status == 404:
            return None
        raise
    return response.get("ETag")


def fetch_from_s3(
    client,
    bucket: str,
    dist_dir: Path,
    datasource: str,
    *,
    target_name: str,
    project_dir: Path | None = None,
) -> bool:
    """Download DuckLake catalog files from S3 and record the ETag.

    Returns True if ducklake.duckdb was found (fetch succeeded).
    """
    from fdl import fdl_target_dir
    from fdl.config import find_project_dir
    from fdl.meta import write_remote_etag

    dist_dir.mkdir(parents=True, exist_ok=True)

    found = _download_file(
        client, bucket, f"{datasource}/{DUCKLAKE_FILE}", dist_dir / DUCKLAKE_FILE
    )

    root = project_dir or find_project_dir()
    state_path = root / fdl_target_dir(target_name) / META_JSON
    etag = _head_catalog_etag(client, bucket, f"{datasource}/{DUCKLAKE_FILE}")
    if etag is None:
        state_path.unlink(missing_ok=True)
    else:
        write_remote_etag(state_path, etag)

    return found
