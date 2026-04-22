"""SQLite WAL mode behavior for v0.9+ local catalogs.

Pins the v0.9.1 fix: DuckLake does not enable WAL on SQLite catalogs by
default, so the FDL ATTACH path now passes META_JOURNAL_MODE='WAL' (and
BUSY_TIMEOUT=5000). These tests confirm:

1. Fresh catalogs created by fdl init land in WAL mode.
2. Catalogs produced by DuckDB -> SQLite conversion land in WAL mode.
3. Legacy v0.9 catalogs still in delete mode auto-migrate to WAL on the
   next FDL command that opens the catalog.
"""

import sqlite3
from pathlib import Path

from typer.testing import CliRunner

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, ducklake_data_path
from fdl.cli import app
from fdl.ducklake import _convert_ducklake_catalog, build_attach_sql


def _journal_mode(catalog_file: Path) -> str:
    conn = sqlite3.connect(str(catalog_file))
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
    conn.close()
    return mode


def _init(fdl_project_dir: Path) -> Path:
    result = CliRunner().invoke(
        app,
        [
            "init",
            "ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            str(fdl_project_dir / "storage"),
            "--target-name",
            "default",
        ],
    )
    assert result.exit_code == 0, result.output
    return fdl_project_dir / ".fdl" / "default" / DUCKLAKE_SQLITE


def test_init_creates_wal_catalog(fdl_project_dir: Path):
    """fdl init produces a SQLite catalog already in WAL mode."""
    catalog = _init(fdl_project_dir)
    assert _journal_mode(catalog) == "wal"


def test_convert_duckdb_to_sqlite_produces_wal(fdl_project_dir: Path):
    """DuckDB -> SQLite conversion (pull path) yields a WAL catalog."""
    catalog = _init(fdl_project_dir)
    dist_dir = catalog.parent
    duckdb_file = dist_dir / DUCKLAKE_FILE

    # init -> sqlite; round-trip through duckdb so we exercise the
    # sqlite destination ATTACH that pull uses.
    _convert_ducklake_catalog(
        catalog,
        duckdb_file,
        src_type="sqlite",
        dst_type="duckdb",
        data_path=ducklake_data_path(str(duckdb_file)),
    )
    catalog.unlink()

    _convert_ducklake_catalog(
        duckdb_file,
        catalog,
        src_type="duckdb",
        dst_type="sqlite",
        data_path=ducklake_data_path(str(catalog)),
    )

    assert _journal_mode(catalog) == "wal"


def test_build_attach_sql_includes_sqlite_options(fdl_project_dir: Path):
    """ATTACH statement for a SQLite catalog carries WAL + busy_timeout."""
    _init(fdl_project_dir)
    stmts = build_attach_sql("default", project_dir=fdl_project_dir)
    attach = next(s for s in stmts if s.startswith("ATTACH "))
    assert "META_JOURNAL_MODE 'WAL'" in attach
    assert "BUSY_TIMEOUT 5000" in attach


def test_existing_delete_mode_catalog_auto_migrates_on_attach(
    fdl_project_dir: Path,
):
    """A v0.9 catalog still in journal_mode=delete flips to WAL on next use."""
    catalog = _init(fdl_project_dir)

    # Simulate a v0.9 catalog that predates v0.9.1 by forcing delete mode.
    conn = sqlite3.connect(str(catalog))
    try:
        conn.execute("PRAGMA journal_mode = DELETE")
    finally:
        conn.close()
    assert _journal_mode(catalog) == "delete"

    # Any FDL command that opens the catalog should flip it to WAL.
    result = CliRunner().invoke(app, ["sql", "default", "SELECT 1"])
    assert result.exit_code == 0, result.output
    assert _journal_mode(catalog) == "wal"
