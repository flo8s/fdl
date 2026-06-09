"""Tests for DuckLake catalog format conversion helpers."""

from pathlib import Path

import duckdb
from typer.testing import CliRunner

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, fdl_target_dir
from fdl.cli import app
from fdl.ducklake import (
    _convert_ducklake_catalog,
    convert_duckdb_to_sqlite,
    convert_sqlite_to_duckdb,
)


def _list_tables(catalog_file: Path, type_: str) -> list[str]:
    """List table names in a raw DuckLake catalog file (duckdb or sqlite)."""
    conn = duckdb.connect(":memory:")
    if type_ == "sqlite":
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        conn.execute(f"ATTACH '{catalog_file}' AS c (TYPE sqlite)")
    else:
        conn.execute(f"ATTACH '{catalog_file}' AS c")
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='c' ORDER BY table_name"
    ).fetchall()
    conn.close()
    return sorted(r[0] for r in rows)


def _init_sqlite_project(root: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "init",
            "ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            str(root / "storage"),
            "--target-name",
            "default",
        ],
    )
    assert result.exit_code == 0, result.output


def test_convert_roundtrip_sqlite_to_duckdb_to_sqlite(fdl_project_dir: Path):
    """SQLite -> DuckDB -> SQLite preserves the full set of DuckLake tables."""
    dist_dir = fdl_project_dir / fdl_target_dir("default")
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE

    _init_sqlite_project(fdl_project_dir)
    assert sqlite_file.exists()

    tables_before = _list_tables(sqlite_file, "sqlite")

    convert_sqlite_to_duckdb(fdl_project_dir, "default")
    assert duckdb_file.exists()
    assert _list_tables(duckdb_file, "duckdb") == tables_before

    sqlite_file.unlink()
    convert_duckdb_to_sqlite(fdl_project_dir, "default")
    assert sqlite_file.exists()
    assert _list_tables(sqlite_file, "sqlite") == tables_before


def test_convert_duckdb_to_sqlite_is_noop_when_sqlite_exists(fdl_project_dir: Path):
    """convert_duckdb_to_sqlite returns early when sqlite already exists."""
    dist_dir = fdl_project_dir / fdl_target_dir("default")

    _init_sqlite_project(fdl_project_dir)
    convert_sqlite_to_duckdb(fdl_project_dir, "default")

    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    sqlite_mtime = sqlite_file.stat().st_mtime

    convert_duckdb_to_sqlite(fdl_project_dir, "default")
    assert sqlite_file.stat().st_mtime == sqlite_mtime
    assert duckdb_file.exists()


def test_convert_duckdb_to_sqlite_is_noop_when_no_duckdb(fdl_project_dir: Path):
    """convert_duckdb_to_sqlite returns early when no duckdb file exists."""
    dist_dir = fdl_project_dir / fdl_target_dir("default")

    _init_sqlite_project(fdl_project_dir)

    convert_duckdb_to_sqlite(fdl_project_dir, "default")
    assert (dist_dir / DUCKLAKE_SQLITE).exists()
    assert not (dist_dir / DUCKLAKE_FILE).exists()


def test_convert_reads_non_main_metadata_schema(tmp_path: Path):
    """A DuckDB catalog whose ducklake_* metadata lives outside 'main'
    (METADATA_SCHEMA) still converts to a usable SQLite catalog.

    DuckDB/PostgreSQL backends may place metadata in any schema, so the catalog
    fetched on ``fdl pull`` is not guaranteed to use 'main'.
    """
    data_dir = tmp_path / "data"
    src = tmp_path / DUCKLAKE_FILE
    dst = tmp_path / DUCKLAKE_SQLITE

    conn = duckdb.connect()
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(
        f"ATTACH 'ducklake:{src}' AS lake "
        f"(DATA_PATH '{data_dir}', METADATA_SCHEMA 'custom_meta')"
    )
    conn.execute("CREATE TABLE lake.main.t (x INTEGER)")
    conn.execute("INSERT INTO lake.main.t VALUES (1), (2), (3)")
    conn.execute("DETACH lake")
    conn.close()

    _convert_ducklake_catalog(
        src,
        dst,
        src_type="duckdb",
        dst_type="sqlite",
        data_path=str(data_dir),
    )

    out = duckdb.connect()
    out.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
    out.execute(
        f"ATTACH 'ducklake:{dst}' AS lake "
        f"(DATA_PATH '{data_dir}', META_TYPE 'sqlite', READ_ONLY)"
    )
    rows = out.execute("SELECT x FROM lake.main.t ORDER BY x").fetchall()
    out.close()
    assert rows == [(1,), (2,), (3,)]
