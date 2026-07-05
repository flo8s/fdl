"""Catalog maintenance: snapshot expiration and data file cleanup.

DuckLake never removes data on its own: every build adds snapshots, and
dropped/replaced tables (including their ``ducklake_inlined_data_*``
tables) stay in the catalog forever. For datasets that rebuild frequently
this grows the catalog linearly with build count, which slows down the
SQLite <-> DuckDB conversion in push/pull.

:func:`expire_snapshots` runs the three DuckLake maintenance functions
against the local SQLite catalog before push:

1. ``ducklake_expire_snapshots`` — drop snapshots older than the retention
   period (the latest snapshot is always kept, even when it is older).
2. ``ducklake_cleanup_old_files`` — delete data files that expired
   snapshots scheduled for deletion.
3. ``ducklake_delete_orphaned_files`` — delete files in DATA_PATH that are
   not tracked by the catalog (leftovers from crashed writes).
"""

from __future__ import annotations

from pathlib import Path

from fdl.console import console


def expire_snapshots(
    target_name: str,
    *,
    retention_days: int,
    project_dir: Path | None = None,
) -> None:
    """Expire old snapshots and delete unreferenced data files.

    Connects to the local SQLite catalog (with the target's DATA_PATH, so
    file cleanup also works for S3 targets) and expires every snapshot
    older than ``retention_days`` days. Data files that become
    unreferenced are deleted from storage in the same pass.

    Args:
        target_name: Target name from fdl.toml.
        retention_days: Snapshots older than this many days are expired.
            ``0`` expires everything except the latest snapshot.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Raises:
        ValueError: If ``retention_days`` is negative.
        FileNotFoundError: If the local catalog file does not exist.
    """
    from fdl.config import datasource_name, find_project_dir
    from fdl.ducklake import _sql_escape, connect

    days = int(retention_days)
    if days < 0:
        raise ValueError(f"retention_days must be >= 0, got {days}")

    root = project_dir or find_project_dir()
    name = _sql_escape(datasource_name(root))
    cutoff = f"now() - INTERVAL '{days} days'"

    with connect(target_name=target_name, project_dir=root) as conn:
        # Each maintenance function is counted with a dry run first, then
        # executed with a plain CALL. Wrapping the real call in a SELECT
        # (e.g. SELECT count(*) FROM ducklake_expire_snapshots(...)) runs
        # it in a read-only transaction: filesystem deletes still happen
        # but the catalog metadata changes are rolled back.
        expired = conn.execute(
            f"SELECT count(*) FROM ducklake_expire_snapshots("
            f"'{name}', older_than => {cutoff}, dry_run => true)"
        ).fetchone()[0]
        if not expired:
            return
        conn.execute(
            f"CALL ducklake_expire_snapshots('{name}', older_than => {cutoff})"
        )

        # File cleanup only runs when this push expired something: both
        # functions below list DATA_PATH, which is a network LIST for S3
        # targets and not worth paying on every push.
        #
        # Files scheduled for deletion by the expiration above are removed
        # immediately (cleanup_all): only snapshots past the retention
        # period reference them, so no supported reader needs them.
        cleaned = conn.execute(
            f"SELECT count(*) FROM ducklake_cleanup_old_files("
            f"'{name}', cleanup_all => true, dry_run => true)"
        ).fetchone()[0]
        if cleaned:
            conn.execute(
                f"CALL ducklake_cleanup_old_files('{name}', cleanup_all => true)"
            )

        # Orphaned files keep the retention cutoff as a safety margin so a
        # file written moments ago is never swept up.
        orphaned = conn.execute(
            f"SELECT count(*) FROM ducklake_delete_orphaned_files("
            f"'{name}', older_than => {cutoff}, dry_run => true)"
        ).fetchone()[0]
        if orphaned:
            conn.execute(
                f"CALL ducklake_delete_orphaned_files("
                f"'{name}', older_than => {cutoff})"
            )

    console.print(
        f"Expired {expired} snapshots (older than {days} days), "
        f"deleted {cleaned + orphaned} data files"
    )
