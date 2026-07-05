"""Integration tests for snapshot expiration on push (local targets).

Spec (docs/reference/cli.md#push):
  fdl push expires snapshots older than maintenance.snapshot_retention_days
  (default 7 days) before converting the catalog, and deletes the data
  files those snapshots referenced. This keeps the catalog from growing
  linearly with build count: without expiration every CREATE OR REPLACE
  leaves a dead table version (and its ducklake_inlined_data_* table)
  behind forever.
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
