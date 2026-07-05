"""Catalog maintenance: snapshot expiration and data file cleanup.

DuckLake in normal operation never removes anything: every build adds
snapshots, and dropped/replaced tables (including their
``ducklake_inlined_data_*`` tables) stay in the catalog forever. For
catalogs that are rebuilt frequently this grows the catalog linearly
with build count, which slows down the SQLite <-> DuckDB conversion in
push/pull and bloats the shipped catalog.

Entry points:

- :func:`expire_snapshots` — the core primitive (CLI: ``fdl expire``,
  Python API: ``fdl.expire``). Expires snapshots older than a retention
  period and deletes the data files that become unreferenced.
- :func:`auto_expire` — policy-driven wrapper, comparable to
  ``git gc --auto``: fdl commands call it after writing to the catalog
  (``run``, ``sql``) and before the catalog conversion in ``push``.
  Controlled by ``maintenance.snapshot_retention_days`` in fdl.toml.

Deliberately NOT used: DuckLake's ``CHECKPOINT``-driven maintenance
(``expire_older_than`` / ``delete_older_than`` catalog options). A
checkpoint also flushes inlined data to parquet, which would turn every
small inlined table into a separate data file on storage — fdl keeps
small tables inlined in the catalog on purpose.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from fdl.console import console


@dataclass(frozen=True)
class ExpireResult:
    """Outcome of one :func:`expire_snapshots` pass."""

    retention_days: int
    expired_snapshots: int
    cleaned_files: int
    orphaned_files: int
    dry_run: bool = False

    @property
    def deleted_files(self) -> int:
        """Total number of data files deleted from storage."""
        return self.cleaned_files + self.orphaned_files


def expire_snapshots(
    target_name: str,
    *,
    retention_days: int,
    dry_run: bool = False,
    always_cleanup: bool = False,
    project_dir: Path | None = None,
) -> ExpireResult:
    """Expire old snapshots and delete unreferenced data files.

    Connects to the local SQLite catalog (with the target's DATA_PATH, so
    file cleanup also works for S3 targets) and expires every snapshot
    older than ``retention_days`` days via ``ducklake_expire_snapshots``.
    The latest snapshot is always kept, even when it is older than the
    cutoff.

    Data files are cleaned up in the same pass:

    - ``ducklake_cleanup_old_files`` deletes the files that expired
      snapshots scheduled for deletion (immediately, ``cleanup_all``:
      only snapshots past the retention period referenced them).
    - ``ducklake_delete_orphaned_files`` deletes untracked files older
      than the cutoff (leftovers from crashed writes); the cutoff keeps a
      freshly written file from being swept up.

    By default file cleanup is skipped when nothing was expired: both
    cleanup functions list DATA_PATH, which is a network LIST for S3
    targets and not worth paying on every automatic invocation. Explicit
    invocations (``fdl expire`` / ``fdl.expire``) pass ``always_cleanup``
    so leftovers are removed even when no snapshot is old enough — the
    replacement for a standalone prune command.

    Args:
        target_name: Target name from fdl.toml.
        retention_days: Snapshots older than this many days are expired.
            ``0`` expires everything except the latest snapshot.
        dry_run: When ``True``, only count; the catalog and storage are
            not modified. File counts are reported only with
            ``always_cleanup`` and are a lower bound: files that the
            expiration itself would schedule for deletion are not known
            until it actually runs.
        always_cleanup: Run the file cleanup even when no snapshot was
            expired.
        project_dir: Project directory containing fdl.toml. Defaults to
            the nearest ancestor that contains one.

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
        if expired and not dry_run:
            conn.execute(
                f"CALL ducklake_expire_snapshots('{name}', older_than => {cutoff})"
            )

        cleaned = orphaned = 0
        if always_cleanup or (expired and not dry_run):
            cleaned = conn.execute(
                f"SELECT count(*) FROM ducklake_cleanup_old_files("
                f"'{name}', cleanup_all => true, dry_run => true)"
            ).fetchone()[0]
            if cleaned and not dry_run:
                conn.execute(
                    f"CALL ducklake_cleanup_old_files("
                    f"'{name}', cleanup_all => true)"
                )

            orphaned = conn.execute(
                f"SELECT count(*) FROM ducklake_delete_orphaned_files("
                f"'{name}', older_than => {cutoff}, dry_run => true)"
            ).fetchone()[0]
            if orphaned and not dry_run:
                conn.execute(
                    f"CALL ducklake_delete_orphaned_files("
                    f"'{name}', older_than => {cutoff})"
                )

    return ExpireResult(days, expired, cleaned, orphaned, dry_run=dry_run)


def auto_expire(target_name: str, *, project_dir: Path | None = None) -> None:
    """Run policy-driven expiration, like ``git gc --auto``.

    Reads ``maintenance.snapshot_retention_days`` from fdl.toml and calls
    :func:`expire_snapshots`. No-op when the policy is disabled
    (``snapshot_retention_days = false``). Prints a one-line summary when
    anything was expired, stays silent otherwise.

    Callers invoke this after an operation that wrote to the catalog
    (see :func:`latest_snapshot_id` for the cheap write detection) or,
    for push, right before the catalog conversion.
    """
    from fdl.config import find_project_dir, snapshot_retention_days

    root = project_dir or find_project_dir()
    retention = snapshot_retention_days(root)
    if retention is None:
        return

    result = expire_snapshots(
        target_name, retention_days=retention, project_dir=root
    )
    if result.expired_snapshots:
        console.print(
            f"Expired {result.expired_snapshots} snapshots "
            f"(older than {result.retention_days} days), "
            f"deleted {result.deleted_files} data files"
        )


def latest_snapshot_id(
    target_name: str, project_dir: Path | None = None
) -> int | None:
    """Latest snapshot id, read directly from the local SQLite catalog.

    Used to bracket a command execution and detect whether it wrote to
    the catalog (writes always create snapshots), so that read-only
    commands never trigger :func:`auto_expire`. Reads the SQLite file
    with the stdlib driver — no DuckLake ATTACH — so it is cheap enough
    to call twice per command.

    Returns ``None`` when the catalog file does not exist.
    """
    from fdl.config import catalog_path

    path = Path(catalog_path(target_name, project_dir))
    if not path.exists():
        return None
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT max(snapshot_id) FROM ducklake_snapshot"
        ).fetchone()[0]
    finally:
        conn.close()
