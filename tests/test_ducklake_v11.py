"""v0.11 ducklake helpers: postgres DSN, build_attach_sql_v11, init."""

from __future__ import annotations

import pytest

from fdl.config import CatalogSpec, PgConnInfo
from fdl.ducklake import (
    _libpq_escape_value,
    build_attach_sql_v11,
    init_ducklake_v11,
    postgres_attach_dsn,
)
from fdl.s3 import S3Config


class TestLibpqEscape:
    def test_plain_alnum(self):
        assert _libpq_escape_value("mydb") == "mydb"

    def test_empty_quoted(self):
        assert _libpq_escape_value("") == "''"

    def test_space_quoted(self):
        assert _libpq_escape_value("my db") == "'my db'"

    def test_single_quote_escaped(self):
        # pa's  →  'pa\'s'
        assert _libpq_escape_value("pa's") == "'pa\\'s'"

    def test_backslash_escaped(self):
        assert _libpq_escape_value("c:\\x") == "'c:\\\\x'"

    def test_tricky_password(self):
        # Designed to stress both quote and backslash escaping
        assert _libpq_escape_value("pa's s") == "'pa\\'s s'"


class TestPostgresAttachDsn:
    def test_minimal(self):
        pg = PgConnInfo(
            host="localhost",
            port=None,
            database="fdl",
            user=None,
            password=None,
            schema=None,
        )
        assert postgres_attach_dsn(pg) == "dbname=fdl host=localhost"

    def test_full(self):
        pg = PgConnInfo(
            host="db.example.com",
            port=5433,
            database="fdl",
            user="alice",
            password="s3cret",
            schema="mart",
        )
        assert (
            postgres_attach_dsn(pg)
            == "dbname=fdl host=db.example.com port=5433 user=alice password=s3cret"
        )

    def test_tricky_password_quoting(self):
        pg = PgConnInfo(
            host="h", port=None, database="db", user="u",
            password="pa's s", schema=None,
        )
        dsn = postgres_attach_dsn(pg)
        assert "password='pa\\'s s'" in dsn


def _sqlite_spec(path: str) -> CatalogSpec:
    return CatalogSpec(scheme="sqlite", raw=f"sqlite:///{path}", path=path)


def _pg_spec(schema: str | None = None) -> CatalogSpec:
    return CatalogSpec(
        scheme="postgres",
        raw="postgres://...",
        pg=PgConnInfo(
            host="h", port=None, database="db",
            user="u", password=None, schema=schema,
        ),
    )


class TestBuildAttachSqlV11:
    def test_sqlite_local_data(self):
        stmts = build_attach_sql_v11(
            metadata=_sqlite_spec("/tmp/x.sqlite"),
            data_url="/tmp/data",
            datasource="ds",
        )
        assert stmts[0] == "INSTALL ducklake; LOAD ducklake;"
        # No postgres/httpfs when not needed
        assert all("postgres" not in s.lower() or "ducklake" not in s.lower() for s in stmts[:2])
        assert "INSTALL httpfs" not in " ".join(stmts)
        # ATTACH includes sqlite: prefix and WAL/BUSY_TIMEOUT options
        attach = next(s for s in stmts if s.startswith("ATTACH"))
        assert "ducklake:sqlite:/tmp/x.sqlite" in attach
        assert "DATA_PATH '/tmp/data'" in attach
        assert "OVERRIDE_DATA_PATH true" in attach
        assert "META_JOURNAL_MODE 'WAL'" in attach
        assert "BUSY_TIMEOUT 5000" in attach
        assert stmts[-1] == "USE ds"

    def test_sqlite_s3_data(self):
        s3 = S3Config(
            bucket="b", endpoint="https://example.com",
            access_key_id="K", secret_access_key="S",
        )
        stmts = build_attach_sql_v11(
            metadata=_sqlite_spec("/tmp/x.sqlite"),
            data_url="s3://b/prefix",
            datasource="ds",
            data_s3_config=s3,
        )
        joined = "\n".join(stmts)
        assert "INSTALL httpfs; LOAD httpfs;" in joined
        assert "CREATE SECRET" in joined
        assert "KEY_ID 'K'" in joined
        assert "SECRET 'S'" in joined

    def test_s3_requires_credentials(self):
        with pytest.raises(ValueError, match="data_s3_config"):
            build_attach_sql_v11(
                metadata=_sqlite_spec("/tmp/x.sqlite"),
                data_url="s3://b/p",
                datasource="ds",
            )

    def test_postgres_prelude_and_schema(self):
        stmts = build_attach_sql_v11(
            metadata=_pg_spec(schema="mart"),
            data_url="/tmp/data",
            datasource="ds",
        )
        joined = "\n".join(stmts)
        assert "INSTALL postgres; LOAD postgres;" in joined
        attach = next(s for s in stmts if s.startswith("ATTACH"))
        assert "ducklake:postgres:dbname=db host=h user=u" in attach
        assert "METADATA_SCHEMA 'mart'" in attach

    def test_postgres_schema_override(self):
        stmts = build_attach_sql_v11(
            metadata=_pg_spec(schema="from_url"),
            data_url="/tmp",
            datasource="ds",
            metadata_schema="explicit",
        )
        attach = next(s for s in stmts if s.startswith("ATTACH"))
        assert "METADATA_SCHEMA 'explicit'" in attach
        assert "'from_url'" not in attach

    def test_read_only_option(self):
        stmts = build_attach_sql_v11(
            metadata=_sqlite_spec("/tmp/x.sqlite"),
            data_url="/tmp/data",
            datasource="ds",
            read_only=True,
        )
        attach = next(s for s in stmts if s.startswith("ATTACH"))
        assert "READ_ONLY" in attach


class TestInitDucklakeV11Sqlite:
    def test_creates_sqlite_catalog(self, tmp_path):
        spec = _sqlite_spec(str(tmp_path / "lake.sqlite"))
        init_ducklake_v11(spec, str(tmp_path / "data"), "ds")
        assert (tmp_path / "lake.sqlite").exists()

    def test_idempotent(self, tmp_path):
        spec = _sqlite_spec(str(tmp_path / "lake.sqlite"))
        init_ducklake_v11(spec, str(tmp_path / "data"), "ds")
        # Second call must not error.
        init_ducklake_v11(spec, str(tmp_path / "data"), "ds")
