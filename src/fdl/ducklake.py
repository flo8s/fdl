"""DuckLake catalog management."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import duckdb

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, ducklake_data_path
from fdl.console import console

# DuckLake ATTACH options applied to SQLite catalogs (v0.9.1+).
#
# META_JOURNAL_MODE sets journal_mode on both freshly-created and
# re-attached SQLite catalogs, so v0.9 catalogs still in the default
# ``delete`` mode auto-migrate to WAL on first attach. BUSY_TIMEOUT is
# per-connection and must be set on every attach; 5 s waits out the
# short lock windows that can occur during concurrent writes and avoids
# surfacing SQLITE_BUSY to callers.
SQLITE_CATALOG_OPTIONS: tuple[str, ...] = (
    "META_JOURNAL_MODE 'WAL'",
    "BUSY_TIMEOUT 5000",
)


def _sql_escape(s: str) -> str:
    """Escape a string for use inside a SQL single-quoted literal."""
    return s.replace("'", "''")


def _is_sqlite_catalog(catalog_file: Path) -> bool:
    """Return True when the path points to an FDL SQLite catalog."""
    return catalog_file.name == DUCKLAKE_SQLITE


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
         (DATA_PATH '<dp>', OVERRIDE_DATA_PATH true[, READ_ONLY]
          [, META_JOURNAL_MODE 'WAL', BUSY_TIMEOUT 5000])
      5. USE <datasource>

    The SQLite-specific options (META_JOURNAL_MODE, BUSY_TIMEOUT) are
    appended whenever the local catalog is SQLite, which covers all v0.9+
    catalogs and auto-migrates any legacy ``delete``-mode file on attach.

    Paths and credentials containing single quotes are SQL-escaped
    (`'` -> `''`). The caller is responsible for any filesystem side
    effects (e.g. creating the local data directory for local targets).

    Args:
        target_name: Target name from fdl.toml. When ``None``, the storage
            defaults to ``.fdl`` (local only).
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
    if _is_sqlite_catalog(ducklake_path):
        opts.extend(SQLITE_CATALOG_OPTIONS)
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
    dist_dir: Path, dataset_dir: Path, *, public_url: str
) -> None:
    """Initialize DuckLake catalog (skip if exists).

    The local catalog is always SQLite (as of v0.9), which allows concurrent
    read/write from separate processes. Remote/shipped catalogs remain in
    DuckDB format; conversion happens in push/pull.
    """
    catalog_file = dist_dir / DUCKLAKE_SQLITE
    if catalog_file.exists():
        console.print(f"DuckLake: [dim]{catalog_file}[/dim]")
        return

    from fdl.config import datasource_name

    datasource = datasource_name(dataset_dir)
    data_path = ducklake_data_path(f"{public_url}/{datasource}/{DUCKLAKE_FILE}")
    console.print(
        f"Creating DuckLake: {datasource} (DATA_PATH: [dim]{data_path}[/dim])"
    )

    dist_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH 'ducklake:{catalog_file}' AS {datasource} (
            DATA_PATH '{data_path}',
            META_TYPE 'sqlite',
            {', '.join(SQLITE_CATALOG_OPTIONS)}
        )
    """)
    conn.close()


def _convert_ducklake_catalog(
    src_file: Path,
    dst_file: Path,
    *,
    src_type: Literal["duckdb", "sqlite"],
    dst_type: Literal["duckdb", "sqlite"],
    data_path: str,
) -> None:
    """Convert a DuckLake catalog between DuckDB and SQLite formats.

    Creates an empty destination catalog via the DuckLake extension (so its
    metadata tables are provisioned), detaches, then raw-attaches both sides
    to copy rows table-by-table. Writes via a ``.tmp`` file and atomically
    renames on success. Any ``.tmp``/WAL leftovers are cleaned up in
    ``finally``.
    """
    SRC = "src"
    DST = "dst"
    tmp_file = dst_file.with_name(dst_file.name + ".tmp")
    leftovers = [
        tmp_file,
        dst_file.with_name(dst_file.name + ".tmp.wal"),
        dst_file.with_name(dst_file.name + ".tmp-wal"),
        dst_file.with_name(dst_file.name + ".tmp-shm"),
        dst_file.with_name(dst_file.name + ".tmp-journal"),
    ]
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        if src_type == "sqlite" or dst_type == "sqlite":
            conn.execute("INSTALL sqlite; LOAD sqlite;")

        dst_opts = [f"DATA_PATH '{data_path}'", f"META_TYPE '{dst_type}'"]
        if dst_type == "sqlite":
            dst_opts.extend(SQLITE_CATALOG_OPTIONS)
        conn.execute(
            f"ATTACH 'ducklake:{tmp_file}' AS {DST} "
            f"({', '.join(dst_opts)})"
        )
        conn.execute(f"DETACH {DST}")

        src_attach = f"ATTACH '{src_file}' AS {SRC}"
        if src_type == "sqlite":
            src_attach += " (TYPE sqlite)"
        conn.execute(src_attach)

        dst_attach = f"ATTACH '{tmp_file}' AS {DST}"
        if dst_type == "sqlite":
            dst_attach += " (TYPE sqlite)"
        conn.execute(dst_attach)

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

        if dst_file.exists():
            dst_file.unlink()
        tmp_file.rename(dst_file)
    finally:
        for f in leftovers:
            if f.exists():
                f.unlink()


def convert_sqlite_to_duckdb(dataset_dir: Path, target_name: str) -> None:
    """Convert SQLite catalog to DuckDB format, replacing ducklake.duckdb."""
    from fdl import fdl_target_dir

    dist_dir = dataset_dir / fdl_target_dir(target_name)
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    if not sqlite_file.exists():
        return

    console.print("Converting DuckLake: SQLite -> DuckDB")
    _convert_ducklake_catalog(
        sqlite_file,
        duckdb_file,
        src_type="sqlite",
        dst_type="duckdb",
        data_path=ducklake_data_path(str(duckdb_file)),
    )


def convert_duckdb_to_sqlite(dataset_dir: Path, target_name: str) -> None:
    """Convert DuckDB catalog to SQLite format, replacing ducklake.sqlite.

    Inverse of :func:`convert_sqlite_to_duckdb`. Used by the v0.8 -> v0.9
    migration and by ``fdl pull`` after the remote DuckDB catalog has been
    downloaded locally.

    Idempotent: returns early when there is nothing to do.
    """
    from fdl import fdl_target_dir

    dist_dir = dataset_dir / fdl_target_dir(target_name)
    duckdb_file = dist_dir / DUCKLAKE_FILE
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    if not duckdb_file.exists():
        return
    if sqlite_file.exists():
        return

    console.print("Converting DuckLake: DuckDB -> SQLite")
    _convert_ducklake_catalog(
        duckdb_file,
        sqlite_file,
        src_type="duckdb",
        dst_type="sqlite",
        data_path=ducklake_data_path(str(sqlite_file)),
    )
