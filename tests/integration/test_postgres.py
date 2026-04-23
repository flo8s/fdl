"""Integration tests for PostgreSQL live catalog (init, publish, pull rejection).

Requires ``FDL_TEST_POSTGRES_URL`` (e.g. ``postgres://postgres:postgres@localhost:5432/fdl_test``).
In CI this is provided by the ``postgres`` service in ``.github/workflows/test.yml``.
Locally, point it at any reachable postgres DB; the tests isolate via unique
per-test schemas and drop them in teardown.
"""

from __future__ import annotations

import os
import urllib.parse
import uuid
from pathlib import Path

import pytest

import fdl
from fdl import FDL_DIR


_PG_BASE_URL = os.environ.get("FDL_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    not _PG_BASE_URL,
    reason="FDL_TEST_POSTGRES_URL not set; skipping postgres integration tests",
)


def _url_with_schema(base: str, schema: str) -> str:
    """Append ``?schema=<schema>`` (or merge into an existing query string)."""
    parts = urllib.parse.urlsplit(base)
    query = dict(urllib.parse.parse_qsl(parts.query))
    query["schema"] = schema
    new_query = urllib.parse.urlencode(query)
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, new_query, parts.fragment)
    )


@pytest.fixture
def pg_schema():
    """Create a unique postgres schema per test; drop it on teardown."""
    assert _PG_BASE_URL is not None
    import psycopg

    parts = urllib.parse.urlsplit(_PG_BASE_URL)
    schema = f"fdl_test_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(
        host=parts.hostname,
        port=parts.port or 5432,
        dbname=(parts.path or "").lstrip("/"),
        user=parts.username,
        password=parts.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()
    finally:
        conn.close()
    try:
        yield schema
    finally:
        conn = psycopg.connect(
            host=parts.hostname,
            port=parts.port or 5432,
            dbname=(parts.path or "").lstrip("/"),
            user=parts.username,
            password=parts.password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
            conn.commit()
        finally:
            conn.close()


@pytest.fixture
def pg_metadata_url(pg_schema):
    """A postgres URL scoped to this test's unique schema."""
    assert _PG_BASE_URL is not None
    return _url_with_schema(_PG_BASE_URL, pg_schema)


def test_init_postgres_creates_metadata_tables(pg_metadata_url, fdl_project_dir):
    """fdl init against postgres provisions the DuckLake metadata tables."""
    import psycopg

    fdl.init(
        "ds",
        metadata_url=pg_metadata_url,
        data_url=str(fdl_project_dir / FDL_DIR / "data"),
        project_dir=fdl_project_dir,
    )

    parts = urllib.parse.urlsplit(pg_metadata_url)
    query = dict(urllib.parse.parse_qsl(parts.query))
    schema = query["schema"]
    conn = psycopg.connect(
        host=parts.hostname,
        port=parts.port or 5432,
        dbname=(parts.path or "").lstrip("/"),
        user=parts.username,
        password=parts.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s",
                (schema,),
            )
            tables = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
    assert "ducklake_metadata" in tables
    assert "ducklake_snapshot" in tables


def test_postgres_attach_write_then_read(pg_metadata_url, fdl_project_dir):
    """Writes committed under one fdl.connect() are visible to a later one."""
    fdl.init(
        "ds",
        metadata_url=pg_metadata_url,
        data_url=str(fdl_project_dir / FDL_DIR / "data"),
        project_dir=fdl_project_dir,
    )

    with fdl.connect(project_dir=fdl_project_dir) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (10), (20), (30)")

    with fdl.connect(project_dir=fdl_project_dir) as conn:
        rows = conn.execute("SELECT x FROM t ORDER BY x").fetchall()
    assert rows == [(10,), (20,), (30,)]


def test_publish_from_postgres_roundtrip(pg_metadata_url, fdl_project_dir, tmp_path):
    """postgres live → fdl publish → fdl pull into sqlite mirror → read."""
    publish_dir = tmp_path / "dist"
    publish_dir.mkdir()

    fdl.init(
        "ds",
        metadata_url=pg_metadata_url,
        data_url=str(fdl_project_dir / FDL_DIR / "data"),
        publish_url=str(publish_dir),
        project_dir=fdl_project_dir,
    )

    with fdl.connect(project_dir=fdl_project_dir) as conn:
        conn.execute("CREATE TABLE t (x INTEGER, label VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    fdl.publish(project_dir=fdl_project_dir)

    assert (publish_dir / "ducklake.duckdb").exists()

    # Mirror project: same [data].url (so parquet files are reachable), pull
    # the frozen into a local sqlite.
    mirror = tmp_path / "mirror"
    mirror.mkdir()
    mirror_sqlite = mirror / FDL_DIR / "ducklake.sqlite"
    src_data = (fdl_project_dir / FDL_DIR / "data").resolve()
    (mirror / "fdl.toml").write_text(
        f'name = "ds"\n\n'
        f"[metadata]\n"
        f'url = "sqlite:///{mirror_sqlite.resolve()}"\n\n'
        f"[data]\n"
        f'url = "{src_data}"\n\n'
        f"[publishes.default]\n"
        f'url = "{publish_dir}"\n'
    )

    fdl.pull(project_dir=mirror)

    with fdl.connect(project_dir=mirror) as conn:
        rows = conn.execute("SELECT x, label FROM t ORDER BY x").fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


def test_pull_rejects_postgres_metadata(pg_metadata_url, fdl_project_dir, tmp_path):
    """fdl pull refuses to touch a PostgreSQL live catalog."""
    publish_dir = tmp_path / "dist"
    publish_dir.mkdir()

    fdl.init(
        "ds",
        metadata_url=pg_metadata_url,
        data_url=str(fdl_project_dir / FDL_DIR / "data"),
        publish_url=str(publish_dir),
        project_dir=fdl_project_dir,
    )

    with pytest.raises(ValueError, match="SQLite metadata"):
        fdl.pull(project_dir=fdl_project_dir)
