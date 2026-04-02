"""DuckLake catalog management."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, FDL_DIR, ducklake_data_path
from fdl.console import console


@contextmanager
def connect(
    *,
    storage: str | None = None,
    target_name: str | None = None,
) -> Generator[duckdb.DuckDBPyConnection]:
    """Connect to the DuckLake catalog and return a DuckDB connection.

    Opens an in-memory DuckDB connection, loads the DuckLake extension,
    and ATTACHes the local catalog (`.fdl/ducklake.duckdb`) with the
    correct `DATA_PATH` and `OVERRIDE_DATA_PATH`.

    When `storage` points to an S3 path, httpfs is loaded and S3 credentials
    are configured from the target config.

    Args:
        storage: Base path for data files.
            Defaults to `FDL_STORAGE` env var, then `.fdl`.
        target_name: Target name for S3 credential resolution.

    Yields:
        A DuckDB connection with the DuckLake catalog attached as the
        dataset name (from `fdl.toml`).

    Raises:
        FileNotFoundError: If `.fdl/ducklake.duckdb` does not exist.

    Examples:
        >>> from fdl.ducklake import connect
        >>> with connect(storage="/tmp/fdl/mydata", target_name="default") as conn:
        ...     conn.execute("CREATE TABLE cities (name VARCHAR, pop INTEGER)")
    """
    from fdl.config import datasource_name

    name = datasource_name()

    import os

    from fdl import fdl_target_dir

    # FDL_CATALOG env var (set by fdl run) takes precedence over target_name
    env_catalog = os.environ.get("FDL_CATALOG")
    if env_catalog:
        ducklake_path = Path(env_catalog)
    else:
        base = fdl_target_dir(target_name) if target_name else FDL_DIR
        ducklake_path = base / DUCKLAKE_FILE
    if not ducklake_path.exists():
        msg = f"{ducklake_path} not found. Run 'fdl init' or 'fdl pull' first."
        raise FileNotFoundError(msg)

    if storage is None:
        from fdl.config import storage as get_storage

        storage = get_storage()
    data_path = ducklake_data_path(f"{storage}/{DUCKLAKE_FILE}")

    # Ensure local storage directory exists for data file writes
    if not storage.startswith("s3://"):
        Path(data_path).mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        if storage.startswith("s3://"):
            from fdl.config import target_s3_config
            from fdl.s3 import configure_duckdb_s3

            if not target_name:
                raise ValueError("target_name is required for S3 storage")
            configure_duckdb_s3(conn, target_s3_config(target_name))
        conn.execute(f"""
            ATTACH 'ducklake:{ducklake_path}' AS {name} (
                DATA_PATH '{data_path}',
                OVERRIDE_DATA_PATH true
            )
        """)
        yield conn
    finally:
        conn.close()


def init_ducklake(
    dist_dir: Path, dataset_dir: Path, *, public_url: str, sqlite: bool = False
) -> None:
    """Initialize DuckLake catalog (skip if exists)."""
    catalog_file = dist_dir / (DUCKLAKE_SQLITE if sqlite else DUCKLAKE_FILE)
    if catalog_file.exists():
        return

    from fdl.config import datasource_name

    datasource = datasource_name(dataset_dir)
    data_path = ducklake_data_path(f"{public_url}/{datasource}/{DUCKLAKE_FILE}")
    meta_type = "sqlite" if sqlite else "duckdb"
    console.print(
        f"Creating DuckLake ({meta_type}): {datasource} (DATA_PATH: [dim]{data_path}[/dim])"
    )

    dist_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH 'ducklake:{catalog_file}' AS {datasource} (
            DATA_PATH '{data_path}',
            META_TYPE '{meta_type}'
        )
    """)
    conn.close()


def convert_sqlite_to_duckdb(dataset_dir: Path, target_name: str) -> None:
    """Convert SQLite catalog to DuckDB format, replacing ducklake.duckdb."""
    from fdl import fdl_target_dir

    dist_dir = dataset_dir / fdl_target_dir(target_name)
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    if not sqlite_file.exists():
        return

    data_path = ducklake_data_path(str(dist_dir / DUCKLAKE_FILE))

    console.print("Converting DuckLake: SQLite -> DuckDB")
    SRC = "src"
    DST = "dst"
    tmp_file = duckdb_file.with_suffix(".duckdb.tmp")
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")

        conn.execute(f"""
            ATTACH 'ducklake:{tmp_file}' AS {DST} (DATA_PATH '{data_path}')
        """)

        conn.execute(f"DETACH {DST}")
        conn.execute(f"ATTACH '{sqlite_file}' AS {SRC} (TYPE sqlite)")
        conn.execute(f"ATTACH '{tmp_file}' AS {DST}")

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog='{SRC}'"
        ).fetchall()
        for (table_name,) in tables:
            conn.execute(f"DELETE FROM {DST}.main.{table_name}")
            conn.execute(
                f"INSERT INTO {DST}.main.{table_name} "
                f"SELECT * FROM {SRC}.main.{table_name}"
            )

        conn.execute(f"CHECKPOINT {DST}")
        conn.close()

        if duckdb_file.exists():
            duckdb_file.unlink()
        tmp_file.rename(duckdb_file)
    finally:
        for f in [tmp_file, tmp_file.with_suffix(".duckdb.tmp.wal")]:
            if f.exists():
                f.unlink()
