"""v0.11 init_project: write fdl.toml (v0.11 schema) and initialize the live catalog."""

from __future__ import annotations

import os
import re
from pathlib import Path

from fdl import FDL_DIR
from fdl.config import PROJECT_CONFIG, PgConnInfo, parse_catalog_url
from fdl.console import console


def _validate_sql_identifier(name: str) -> None:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    if sanitized != name:
        raise ValueError(
            f"'{name}' is not a valid SQL identifier. Use '{sanitized}' instead."
        )


def _default_metadata_url(project_dir: Path) -> str:
    sqlite_path = (project_dir / FDL_DIR / "ducklake.sqlite").resolve().as_posix()
    return f"sqlite:///{sqlite_path}"


def _default_data_url(project_dir: Path) -> str:
    return str((project_dir / FDL_DIR / "data").resolve())


def _ensure_postgres_schema(pg: PgConnInfo, schema: str) -> None:
    """Ensure the postgres database is reachable and the schema exists.

    The database itself must exist (fdl does not CREATE DATABASE since that
    requires superuser privileges and runs outside a transaction). Schema
    creation uses ``CREATE SCHEMA IF NOT EXISTS`` via a safely-quoted
    identifier.
    """
    import psycopg
    from psycopg import sql

    try:
        conn = psycopg.connect(
            host=pg.host,
            port=pg.port or 5432,
            dbname=pg.database,
            user=pg.user,
            password=pg.password,
        )
    except psycopg.OperationalError as e:
        raise RuntimeError(
            f"Cannot connect to postgres database {pg.database!r} "
            f"on {pg.host}: {e}\n"
            f"Create the database first:  CREATE DATABASE {pg.database};"
        ) from e
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
        conn.commit()
    finally:
        conn.close()


def _write_v11_toml(
    path: Path,
    *,
    name: str,
    metadata_url: str,
    data_url: str,
    publish_url: str | None,
    publish_name: str,
) -> None:
    lines = [
        f'name = "{name}"',
        "",
        "[metadata]",
        f'url = "{metadata_url}"',
        "",
        "[data]",
        f'url = "{data_url}"',
    ]
    if publish_url:
        lines += [
            "",
            f"[publishes.{publish_name}]",
            f'url = "{publish_url}"',
        ]
    path.write_text("\n".join(lines) + "\n")


def init_project_v11(
    name: str,
    *,
    metadata_url: str | None = None,
    data_url: str | None = None,
    publish_url: str | None = None,
    publish_name: str = "default",
    project_dir: Path | None = None,
) -> None:
    """Initialize a v0.11 fdl project.

    Writes ``fdl.toml`` with the new schema ([metadata] / [data] / optional
    [publishes.<publish_name>]) and provisions the live catalog:

    - sqlite/duckdb: the catalog file is created on disk.
    - postgres: the database must already exist; the schema is created via
      ``CREATE SCHEMA IF NOT EXISTS``.
    """
    import shutil

    from fdl.ducklake import init_ducklake_v11

    _validate_sql_identifier(name)

    root = project_dir or Path.cwd()
    config_path = root / PROJECT_CONFIG
    if config_path.exists():
        raise FileExistsError(f"{config_path} already exists")

    if metadata_url is None:
        metadata_url = _default_metadata_url(root)
    if data_url is None:
        data_url = _default_data_url(root)

    # ${VAR} expansion happens here, once, at the boundary.
    expanded_metadata = os.path.expandvars(metadata_url)
    expanded_data = os.path.expandvars(data_url)
    spec = parse_catalog_url(expanded_metadata)

    created_fdl_dir = not (root / FDL_DIR).exists()
    try:
        if spec.scheme == "postgres":
            assert spec.pg is not None
            schema = spec.pg.schema or name
            _ensure_postgres_schema(spec.pg, schema)

        _write_v11_toml(
            config_path,
            name=name,
            metadata_url=metadata_url,
            data_url=data_url,
            publish_url=publish_url,
            publish_name=publish_name,
        )

        init_ducklake_v11(
            spec,
            expanded_data,
            name,
            metadata_schema=(spec.pg.schema if spec.pg else None) or name
            if spec.scheme == "postgres"
            else None,
        )
    except Exception:
        if config_path.exists():
            config_path.unlink()
        fdl_dir = root / FDL_DIR
        if created_fdl_dir and fdl_dir.exists():
            shutil.rmtree(fdl_dir, ignore_errors=True)
        raise

    console.print(f"[green]Initialized fdl project: {name}[/green]")
