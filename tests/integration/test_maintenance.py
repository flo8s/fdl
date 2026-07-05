"""Integration tests for snapshot expiration (local targets).

Spec (docs/reference/cli.md#expire):
  fdl expire TARGET [--retention-days N] [--dry-run]
  - Expires snapshots older than the retention period (default:
    maintenance.snapshot_retention_days, or 7 days) and deletes the data
    files they referenced. The latest snapshot is always kept.
  - Runs automatically after fdl commands that wrote to the catalog
    (run, sql) and before the catalog conversion in push. Read-only
    commands never trigger it. maintenance.snapshot_retention_days =
    false disables the automatic runs.

  This keeps the catalog from growing linearly with build count: without
  expiration every CREATE OR REPLACE leaves a dead table version (and
  its ducklake_inlined_data_* table) behind forever.
"""

import sqlite3
from pathlib import Path

from typer.testing import CliRunner

import fdl
from fdl.cli import app


def _init_project(project_dir: Path, storage: Path) -> None:
    result = CliRunner().invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    assert result.exit_code == 0, result.output


def _build(project_dir: Path, build: int, *, rows: int = 5) -> None:
    """Simulate one pipeline build: CREATE OR REPLACE a few tables."""
    with fdl.connect("default", project_dir=project_dir) as conn:
        for i in range(2):
            conn.execute(
                f"CREATE OR REPLACE TABLE t{i} AS "
                f"SELECT range AS x, {build} AS build FROM range({rows})"
            )
        # Large enough to be written as a parquet data file, not inlined
        conn.execute(
            "CREATE OR REPLACE TABLE big AS "
            f"SELECT range AS x, {build} AS build FROM range(100000)"
        )


def _catalog_stats(project_dir: Path) -> dict[str, int]:
    """Read raw counts from the local SQLite catalog."""
    con = sqlite3.connect(project_dir / ".fdl" / "default" / "ducklake.sqlite")
    try:
        return {
            "snapshots": con.execute(
                "SELECT count(*) FROM ducklake_snapshot"
            ).fetchone()[0],
            "tables": con.execute(
                "SELECT count(*) FROM ducklake_table"
            ).fetchone()[0],
            "inlined": con.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'ducklake_inlined_data%'"
            ).fetchone()[0],
        }
    finally:
        con.close()


def test_push_expires_old_snapshots_and_dead_tables(fdl_project_dir: Path):
    """With retention 0, push keeps only the latest snapshot and drops
    dead table versions, including their ducklake_inlined_data_* tables."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(3):
        _build(fdl_project_dir, build)

    before = _catalog_stats(fdl_project_dir)
    assert before["snapshots"] > 1
    assert before["tables"] > 3  # dead versions of t0, t1, big

    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    after = _catalog_stats(fdl_project_dir)
    assert after["snapshots"] == 1
    assert after["tables"] == 3  # current t0, t1, big only
    assert after["inlined"] < before["inlined"]

    # Current data survives expiration
    with fdl.connect("default", project_dir=fdl_project_dir) as conn:
        rows, build = conn.execute(
            "SELECT count(*), max(build) FROM big"
        ).fetchone()
        assert (rows, build) == (100000, 2)


def test_push_deletes_expired_data_files(fdl_project_dir: Path):
    """Parquet files referenced only by expired snapshots are deleted."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(3):
        _build(fdl_project_dir, build)

    data_dir = storage / "test_ds" / "ducklake.duckdb.files"
    assert len(list(data_dir.rglob("*.parquet"))) == 3  # one big per build

    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    assert len(list(data_dir.rglob("*.parquet"))) == 1


def test_push_keeps_snapshots_within_retention(fdl_project_dir: Path):
    """With the default 7-day retention, fresh snapshots are not expired."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)

    before = _catalog_stats(fdl_project_dir)
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir) == before


def test_push_expiration_disabled(fdl_project_dir: Path):
    """snapshot_retention_days = false skips expiration entirely."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)

    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "false",
    ])
    before = _catalog_stats(fdl_project_dir)
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir) == before


def test_expire_command_with_retention_zero(fdl_project_dir: Path):
    """fdl expire --retention-days 0 keeps only the latest snapshot."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(3):
        _build(fdl_project_dir, build)

    result = CliRunner().invoke(app, ["expire", "default", "--retention-days", "0"])
    assert result.exit_code == 0, result.output
    assert "Expired" in result.output

    after = _catalog_stats(fdl_project_dir)
    assert after["snapshots"] == 1
    assert after["tables"] == 3


def test_expire_command_dry_run_changes_nothing(fdl_project_dir: Path):
    """--dry-run reports the would-be count without modifying anything."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(3):
        _build(fdl_project_dir, build)

    before = _catalog_stats(fdl_project_dir)
    data_dir = storage / "test_ds" / "ducklake.duckdb.files"
    files_before = len(list(data_dir.rglob("*.parquet")))

    result = CliRunner().invoke(app, [
        "expire", "default", "--retention-days", "0", "--dry-run",
    ])
    assert result.exit_code == 0, result.output
    assert f"Would expire {before['snapshots'] - 1} snapshots" in result.output

    assert _catalog_stats(fdl_project_dir) == before
    assert len(list(data_dir.rglob("*.parquet"))) == files_before


def test_expire_command_python_api(fdl_project_dir: Path):
    """fdl.expire() returns an ExpireResult with the expired counts."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)

    before = _catalog_stats(fdl_project_dir)
    result = fdl.expire(
        "default", retention_days=0, project_dir=fdl_project_dir
    )
    assert result.expired_snapshots == before["snapshots"] - 1
    assert result.deleted_files > 0
    assert _catalog_stats(fdl_project_dir)["snapshots"] == 1


def test_sql_write_triggers_auto_expire(fdl_project_dir: Path):
    """A writing fdl sql triggers auto-expiration afterwards."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)
    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])

    result = CliRunner().invoke(app, [
        "sql", "default", "CREATE OR REPLACE TABLE t0 AS SELECT 99 AS x",
    ])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir)["snapshots"] == 1


def test_sql_read_does_not_trigger_auto_expire(fdl_project_dir: Path):
    """A read-only fdl sql never expires, even with expirable snapshots."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)
    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])

    before = _catalog_stats(fdl_project_dir)
    result = CliRunner().invoke(app, ["sql", "default", "SELECT count(*) FROM big"])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir) == before


WRITE_SNIPPET = (
    "from fdl.ducklake import connect\n"
    "with connect(target_name='default') as conn:\n"
    "    conn.execute('CREATE OR REPLACE TABLE t0 AS SELECT 99 AS x')\n"
)


def test_run_write_triggers_auto_expire(fdl_project_dir: Path):
    """A pipeline command that wrote to the catalog triggers auto-expiration."""
    import sys

    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)
    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])

    result = CliRunner().invoke(app, [
        "run", "default", "--", sys.executable, "-c", WRITE_SNIPPET,
    ])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir)["snapshots"] == 1


def test_run_without_write_does_not_trigger_auto_expire(fdl_project_dir: Path):
    """A command that did not write to the catalog never expires."""
    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    for build in range(2):
        _build(fdl_project_dir, build)
    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])

    before = _catalog_stats(fdl_project_dir)
    result = CliRunner().invoke(app, ["run", "default", "--", "sh", "-c", "true"])
    assert result.exit_code == 0, result.output

    assert _catalog_stats(fdl_project_dir) == before


def test_push_deletes_old_orphaned_files(fdl_project_dir: Path):
    """Untracked files in DATA_PATH older than the cutoff are deleted."""
    import os
    import time

    storage = fdl_project_dir / "storage"
    _init_project(fdl_project_dir, storage)
    _build(fdl_project_dir, 0)

    data_dir = storage / "test_ds" / "ducklake.duckdb.files"
    orphan = data_dir / "orphan.parquet"
    orphan.write_bytes(b"not a real parquet")
    # Backdate mtime so the older_than cutoff (now, with retention 0)
    # is strictly after the file's timestamp
    old = time.time() - 3600
    os.utime(orphan, (old, old))

    CliRunner().invoke(app, [
        "config", "maintenance.snapshot_retention_days", "0",
    ])
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    assert not orphan.exists()
