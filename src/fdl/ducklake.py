"""DuckLake catalog management."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, ducklake_data_path
from fdl.console import console


def _sql_escape(s: str) -> str:
    """Escape a string for use inside a SQL single-quoted literal."""
    return s.replace("'", "''")


def build_attach_sql(
    target_name: str | None = None,
    *,
    read_only: bool = False,
    project_dir: Path | None = None,
) -> list[str]:
    """Build the SQL statements to open a target's DuckLake catalog.

    Returns the statements in execution order:
      1. INSTALL ducklake; LOAD ducklake;
      2. (S3 only) INSTALL httpfs; LOAD httpfs;
      3. (S3 only) CREATE SECRET (TYPE s3, ...)
      4. ATTACH 'ducklake:<path>' AS <datasource>
         (DATA_PATH '<dp>', OVERRIDE_DATA_PATH true[, READ_ONLY])
      5. USE <datasource>

    Paths and credentials containing single quotes are SQL-escaped
    (`'` -> `''`). The caller is responsible for any filesystem side
    effects (e.g. creating the local data directory for local targets).

    Args:
        target_name: Target name from fdl.toml. When ``None``, the storage
            is resolved from ``FDL_STORAGE`` / default ``.fdl`` (local only).
        read_only: When ``True``, append ``READ_ONLY`` to the ATTACH options.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Raises:
        FileNotFoundError: If the local catalog file does not exist.
        ValueError: If storage resolves to S3 but ``target_name`` is ``None``.
    """
    from fdl.config import (
        catalog_path,
        datasource_name,
        find_project_dir,
        storage as get_storage,
        target_s3_config,
        target_storage_url,
    )

    root = project_dir or find_project_dir()
    name = datasource_name(root)
    ducklake_path = Path(catalog_path(target_name, root))
    if not ducklake_path.exists():
        raise FileNotFoundError(
            f"{ducklake_path} not found. Run 'fdl init' or 'fdl pull' first."
        )

    if target_name is not None:
        storage_val = target_storage_url(target_name, root)
    else:
        storage_val = get_storage(None)
    dp = ducklake_data_path(f"{storage_val}/{DUCKLAKE_FILE}")
    is_s3 = storage_val.startswith("s3://")

    stmts: list[str] = ["INSTALL ducklake; LOAD ducklake;"]
    if is_s3:
        if target_name is None:
            raise ValueError("target_name is required for S3 storage")
        s3 = target_s3_config(target_name, root)
        stmts.append("INSTALL httpfs; LOAD httpfs;")
        stmts.append(
            f"CREATE SECRET (TYPE s3, "
            f"KEY_ID '{_sql_escape(s3.access_key_id)}', "
            f"SECRET '{_sql_escape(s3.secret_access_key)}', "
            f"ENDPOINT '{_sql_escape(s3.endpoint_host)}', "
            f"URL_STYLE 'path', REGION 'auto')"
        )

    opts = [f"DATA_PATH '{_sql_escape(dp)}'", "OVERRIDE_DATA_PATH true"]
    if read_only:
        opts.append("READ_ONLY")
    stmts.append(
        f"ATTACH 'ducklake:{_sql_escape(str(ducklake_path))}' AS {name} "
        f"({', '.join(opts)})"
    )
    stmts.append(f"USE {name}")
    return stmts


@contextmanager
def connect(
    *,
    target_name: str | None = None,
    project_dir: Path | None = None,
) -> Generator[duckdb.DuckDBPyConnection]:
    """Connect to the DuckLake catalog and return a DuckDB connection.

    Opens an in-memory DuckDB connection, loads the DuckLake extension,
    ATTACHes the local catalog (`.fdl/{target}/ducklake.duckdb`) with the
    correct `DATA_PATH` / `OVERRIDE_DATA_PATH`, and selects the datasource
    via `USE`.

    For S3 targets, httpfs is loaded and S3 credentials are configured
    from the target config.

    Args:
        target_name: Target name for storage / S3 credential resolution.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Yields:
        A DuckDB connection with the DuckLake catalog attached as the
        dataset name (from `fdl.toml`) and selected via `USE`.

    Raises:
        FileNotFoundError: If the local catalog file does not exist.
    """
    from fdl.config import (
        find_project_dir,
        storage as get_storage,
        target_storage_url,
    )

    root = project_dir or find_project_dir()

    if target_name is not None:
        storage_val = target_storage_url(target_name, root)
    else:
        storage_val = get_storage(None)

    # Ensure local storage directory exists for data file writes
    if not storage_val.startswith("s3://"):
        local_dp = ducklake_data_path(f"{storage_val}/{DUCKLAKE_FILE}")
        Path(local_dp).mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect()
    try:
        for stmt in build_attach_sql(target_name, project_dir=root):
            conn.execute(stmt)
        yield conn
    finally:
        conn.close()


def init_ducklake(
    dist_dir: Path, dataset_dir: Path, *, public_url: str, sqlite: bool = False
) -> None:
    """Initialize DuckLake catalog (skip if exists)."""
    catalog_file = dist_dir / (DUCKLAKE_SQLITE if sqlite else DUCKLAKE_FILE)
    if catalog_file.exists():
        console.print(f"DuckLake: [dim]{catalog_file}[/dim]")
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
