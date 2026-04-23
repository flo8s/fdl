"""v0.11 config helpers: [metadata] / [data] / [publishes] schema."""

from __future__ import annotations

import pytest

from fdl.config import (
    CatalogSpec,
    PgConnInfo,
    data_url_v11,
    metadata_schema,
    metadata_spec,
    metadata_url,
    parse_catalog_url,
    publish_names,
    publish_public_url,
    publish_url,
    resolve_publish_name,
)


def _write(path, content: str) -> None:
    path.write_text(content)


class TestParseCatalogURL:
    def test_sqlite_relative(self):
        spec = parse_catalog_url("sqlite:///./foo.sqlite")
        assert spec.scheme == "sqlite"
        assert spec.path == "./foo.sqlite"
        assert spec.pg is None

    def test_sqlite_absolute(self):
        spec = parse_catalog_url("sqlite:////abs/path.sqlite")
        assert spec.scheme == "sqlite"
        assert spec.path == "/abs/path.sqlite"

    def test_postgres_full(self):
        spec = parse_catalog_url(
            "postgres://alice:s3cret@db.example.com:5433/fdl?schema=mart"
        )
        assert spec.scheme == "postgres"
        assert spec.pg == PgConnInfo(
            host="db.example.com",
            port=5433,
            database="fdl",
            user="alice",
            password="s3cret",
            schema="mart",
        )

    def test_postgres_minimal(self):
        spec = parse_catalog_url("postgres://localhost/fdl")
        assert spec.scheme == "postgres"
        assert spec.pg.host == "localhost"
        assert spec.pg.port is None
        assert spec.pg.database == "fdl"
        assert spec.pg.user is None
        assert spec.pg.password is None
        assert spec.pg.schema is None

    def test_postgres_password_with_special_chars(self):
        # pa's s → URL-encoded as pa%27s%20s
        spec = parse_catalog_url("postgres://u:pa%27s%20s@h/db")
        assert spec.pg.password == "pa's s"

    def test_postgresql_alias(self):
        spec = parse_catalog_url("postgresql://h/db")
        assert spec.scheme == "postgres"

    def test_postgres_missing_db_errors(self):
        with pytest.raises(ValueError, match="missing database"):
            parse_catalog_url("postgres://h")

    def test_unknown_scheme_errors(self):
        with pytest.raises(ValueError, match="Unsupported"):
            parse_catalog_url("mysql://h/db")

    def test_no_scheme_errors(self):
        with pytest.raises(ValueError, match="must include a scheme"):
            parse_catalog_url("/abs/path.sqlite")


class TestMetadataHelpers:
    def test_metadata_url_expands_vars(self, fdl_project_dir, monkeypatch):
        monkeypatch.setenv("MY_DB", "/tmp/x.sqlite")
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[metadata]\nurl = "sqlite://${MY_DB}"\n',
        )
        assert metadata_url(fdl_project_dir) == "sqlite:///tmp/x.sqlite"

    def test_metadata_spec(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[metadata]\nurl = "postgres://h/db"\n',
        )
        spec = metadata_spec(fdl_project_dir)
        assert isinstance(spec, CatalogSpec)
        assert spec.scheme == "postgres"

    def test_metadata_missing_errors(self, fdl_project_dir):
        _write(fdl_project_dir / "fdl.toml", 'name = "x"\n')
        with pytest.raises(KeyError, match=r"\[metadata\]\.url"):
            metadata_url(fdl_project_dir)

    def test_metadata_schema_override(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[metadata]\nurl = "sqlite:///x"\nschema = "s1"\n',
        )
        assert metadata_schema(fdl_project_dir) == "s1"

    def test_metadata_schema_absent(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[metadata]\nurl = "sqlite:///x"\n',
        )
        assert metadata_schema(fdl_project_dir) is None


class TestDataURL:
    def test_data_url_expands(self, fdl_project_dir, monkeypatch):
        monkeypatch.setenv("BUCKET", "mybucket")
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[data]\nurl = "s3://${BUCKET}/data"\n',
        )
        assert data_url_v11(fdl_project_dir) == "s3://mybucket/data"

    def test_data_missing_errors(self, fdl_project_dir):
        _write(fdl_project_dir / "fdl.toml", 'name = "x"\n')
        with pytest.raises(KeyError, match=r"\[data\]\.url"):
            data_url_v11(fdl_project_dir)


class TestPublishes:
    def test_publish_names_order(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n'
            '[publishes.alpha]\nurl = "a"\n'
            '[publishes.beta]\nurl = "b"\n',
        )
        assert publish_names(fdl_project_dir) == ["alpha", "beta"]

    def test_publish_names_empty(self, fdl_project_dir):
        _write(fdl_project_dir / "fdl.toml", 'name = "x"\n')
        assert publish_names(fdl_project_dir) == []

    def test_publish_url_expands(self, fdl_project_dir, monkeypatch):
        monkeypatch.setenv("U", "https://pub.example.com")
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.default]\nurl = "${U}"\n',
        )
        assert publish_url("default", fdl_project_dir) == "https://pub.example.com"

    def test_publish_public_url(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n'
            '[publishes.default]\n'
            'url = "s3://b/p"\n'
            'public_url = "https://cdn.example.com"\n',
        )
        assert (
            publish_public_url("default", fdl_project_dir)
            == "https://cdn.example.com"
        )

    def test_publish_public_url_absent(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.default]\nurl = "s3://b/p"\n',
        )
        assert publish_public_url("default", fdl_project_dir) is None

    def test_publish_url_unknown_errors(self, fdl_project_dir):
        _write(fdl_project_dir / "fdl.toml", 'name = "x"\n')
        with pytest.raises(KeyError):
            publish_url("missing", fdl_project_dir)


class TestResolvePublishName:
    def test_single_implicit(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.only]\nurl = "a"\n',
        )
        assert resolve_publish_name(None, fdl_project_dir) == "only"

    def test_multiple_requires_explicit(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.a]\nurl = "x"\n[publishes.b]\nurl = "y"\n',
        )
        with pytest.raises(ValueError, match="Multiple publishes"):
            resolve_publish_name(None, fdl_project_dir)

    def test_empty_errors(self, fdl_project_dir):
        _write(fdl_project_dir / "fdl.toml", 'name = "x"\n')
        with pytest.raises(KeyError, match="No \\[publishes"):
            resolve_publish_name(None, fdl_project_dir)

    def test_explicit_unknown_errors(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.only]\nurl = "a"\n',
        )
        with pytest.raises(KeyError):
            resolve_publish_name("missing", fdl_project_dir)

    def test_explicit_hit(self, fdl_project_dir):
        _write(
            fdl_project_dir / "fdl.toml",
            'name = "x"\n[publishes.a]\nurl = "x"\n[publishes.b]\nurl = "y"\n',
        )
        assert resolve_publish_name("b", fdl_project_dir) == "b"
