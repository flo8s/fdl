"""DuckLake catalog management."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import duckdb

from fdl.config import CatalogSpec, PgConnInfo
from fdl.console import console

# DuckLake ATTACH options applied to SQLite catalogs.
#
# META_JOURNAL_MODE puts newly-created and re-attached SQLite catalogs in WAL
# mode for concurrent reader support. BUSY_TIMEOUT is per-connection and
# must be set on every attach; 5 s waits out short lock windows during
# concurrent writes and keeps SQLITE_BUSY from surfacing to callers.
SQLITE_CATALOG_OPTIONS: tuple[str, ...] = (
    "META_JOURNAL_MODE 'WAL'",
    "BUSY_TIMEOUT 5000",
)


def _sql_escape(s: str) -> str:
    """Escape a string for use inside a SQL single-quoted literal."""
    return s.replace("'", "''")


# ---------------------------------------------------------------------------
# Postgres DSN / ATTACH helpers
# ---------------------------------------------------------------------------


def _libpq_escape_value(value: str) -> str:
    """Escape a value for a libpq ``key=value`` pair.

    libpq requires values containing whitespace or special characters to be
    single-quoted, with embedded single quotes and backslashes escaped by a
    preceding backslash.
    """
    if value == "" or any(c in value for c in " \t\n\"'\\"):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    return value


def postgres_attach_dsn(pg: PgConnInfo) -> str:
    """Build a libpq DSN string from parsed PgConnInfo components."""
    parts = [f"dbname={_libpq_escape_value(pg.database)}"]
    if pg.host:
        parts.append(f"host={_libpq_escape_value(pg.host)}")
    if pg.port is not None:
        parts.append(f"port={pg.port}")
    if pg.user:
        parts.append(f"user={_libpq_escape_value(pg.user)}")
    if pg.password is not None:
        parts.append(f"password={_libpq_escape_value(pg.password)}")
    return " ".join(parts)


def _ducklake_attach_target(spec: CatalogSpec) -> str:
    """The string placed inside ``ATTACH 'ducklake:<here>' AS ...``."""
    if spec.scheme == "sqlite":
        if spec.path is None:
            raise ValueError("sqlite catalog spec missing path")
        return f"sqlite:{spec.path}"
    if spec.scheme == "postgres":
        if spec.pg is None:
            raise ValueError("postgres catalog spec missing connection info")
        return f"postgres:{postgres_attach_dsn(spec.pg)}"
    if spec.scheme == "duckdb":
        if spec.path is None:
            raise ValueError("duckdb catalog spec missing path")
        return spec.path
    raise ValueError(f"Unsupported catalog scheme: {spec.scheme}")


# ---------------------------------------------------------------------------
# Build ATTACH SQL for opening a live catalog
# ---------------------------------------------------------------------------


def build_attach_sql(
    *,
    metadata: CatalogSpec,
    data_url: str,
    datasource: str,
    read_only: bool = False,
    metadata_schema: str | None = None,
    data_s3_config: object | None = None,
) -> list[str]:
    """Build the ordered SQL to open a DuckLake catalog.

    1. INSTALL/LOAD ducklake (and postgres/httpfs when applicable)
    2. CREATE SECRET for S3 data storage (if data_url is s3://)
    3. ATTACH 'ducklake:<metadata>' AS <datasource> (DATA_PATH '...', ...)
    4. USE <datasource>
    """
    stmts: list[str] = ["INSTALL ducklake; LOAD ducklake;"]
    if metadata.scheme == "postgres":
        stmts.append("INSTALL postgres; LOAD postgres;")

    is_s3 = data_url.startswith("s3://")
    if is_s3:
        if data_s3_config is None:
            raise ValueError("data_s3_config is required for s3:// data_url")
        stmts.append("INSTALL httpfs; LOAD httpfs;")
        stmts.append(
            "CREATE SECRET (TYPE s3, "
            f"KEY_ID '{_sql_escape(data_s3_config.access_key_id)}', "
            f"SECRET '{_sql_escape(data_s3_config.secret_access_key)}', "
            f"ENDPOINT '{_sql_escape(data_s3_config.endpoint_host)}', "
            "URL_STYLE 'path', REGION 'auto')"
        )

    opts = [
        f"DATA_PATH '{_sql_escape(data_url)}'",
        "OVERRIDE_DATA_PATH true",
    ]
    if metadata.scheme == "sqlite":
        opts.extend(SQLITE_CATALOG_OPTIONS)
    schema = metadata_schema or (
        metadata.pg.schema if metadata.scheme == "postgres" and metadata.pg else None
    )
    if metadata.scheme == "postgres" and schema:
        opts.append(f"METADATA_SCHEMA '{_sql_escape(schema)}'")
    if read_only:
        opts.append("READ_ONLY")

    target = _ducklake_attach_target(metadata)
    stmts.append(
        f"ATTACH 'ducklake:{_sql_escape(target)}' AS {datasource} "
        f"({', '.join(opts)})"
    )
    stmts.append(f"USE {datasource}")
    return stmts


# ---------------------------------------------------------------------------
# Initialize a new live catalog
# ---------------------------------------------------------------------------


def init_ducklake(
    spec: CatalogSpec,
    data_path: str,
    datasource: str,
    *,
    metadata_schema: str | None = None,
    data_s3_config: object | None = None,
) -> None:
    """Initialize a live DuckLake catalog (idempotent for file-based backends).

    For sqlite/duckdb: creates the catalog file if missing.
    For postgres: the database must already exist, and the schema must be
    created by the caller (see :func:`fdl.init_project._ensure_postgres_schema`).
    """
    if spec.scheme in ("sqlite", "duckdb"):
        if spec.path is None:
            raise ValueError(f"{spec.scheme} spec missing path")
        p = Path(spec.path)
        if p.exists():
            console.print(f"DuckLake: [dim]{p}[/dim]")
            return
        p.parent.mkdir(parents=True, exist_ok=True)

    console.print(
        f"Creating DuckLake: {datasource} (DATA_PATH: [dim]{data_path}[/dim])"
    )

    conn = duckdb.connect(":memory:")
    try:
        for stmt in build_attach_sql(
            metadata=spec,
            data_url=data_path,
            datasource=datasource,
            metadata_schema=metadata_schema,
            data_s3_config=data_s3_config,
        ):
            conn.execute(stmt)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Connect: open the live catalog via DuckDB
# ---------------------------------------------------------------------------


@contextmanager
def connect(
    *,
    read_only: bool = False,
    project_dir: Path | None = None,
) -> Generator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection with the live DuckLake catalog attached.

    The catalog is resolved from fdl.toml (``[metadata]``/``[data]``). The
    datasource (``name`` field) is attached and selected via ``USE``.
    """
    from fdl.config import (
        data_s3_config,
        data_url,
        datasource_name,
        find_project_dir,
        metadata_schema,
        metadata_spec,
    )

    root = project_dir or find_project_dir()
    spec = metadata_spec(root)
    d_url = data_url(root)
    datasource = datasource_name(root)

    if not d_url.startswith("s3://"):
        Path(d_url).mkdir(parents=True, exist_ok=True)

    s3 = data_s3_config(root)

    conn = duckdb.connect()
    try:
        for stmt in build_attach_sql(
            metadata=spec,
            data_url=d_url,
            datasource=datasource,
            read_only=read_only,
            metadata_schema=metadata_schema(root),
            data_s3_config=s3,
        ):
            conn.execute(stmt)
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Catalog conversion between sqlite and duckdb (used by clone / publish).
# ---------------------------------------------------------------------------


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

        dst_opts = [f"DATA_PATH '{_sql_escape(data_path)}'", f"META_TYPE '{dst_type}'"]
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
