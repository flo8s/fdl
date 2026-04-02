"""Unit tests for fdl/config.py.

Tests the config resolution logic that all CLI commands depend on:
target resolution, env var dict construction, path derivation, TOML I/O.
"""

from pathlib import Path

import pytest

from fdl.config import (
    catalog_path,
    data_path,
    datasource_name,
    ducklake_url,
    fdl_env_dict,
    get_all,
    resolve_target,
    set_value,
    storage,
    target_public_url,
    target_s3_config,
)


def _setup_target(project_dir, name="default", url="s3://my-bucket", **extra):
    """Write a target config to fdl.toml."""
    path = project_dir / "fdl.toml"
    set_value(f"targets.{name}.url", url, path)
    for k, v in extra.items():
        set_value(f"targets.{name}.{k}", v, path)


# ---------------------------------------------------------------------------
# set_value / get_all — TOML round-trip
# Spec: fdl config stores settings in fdl.toml with dotted keys up to 3 levels
# ---------------------------------------------------------------------------


def test_set_value_creates_new_file(fdl_project_dir):
    """set_value creates fdl.toml if it doesn't exist."""
    path = fdl_project_dir / "fdl.toml"
    set_value("name", "test", path)
    assert path.exists()


def test_set_value_three_level_key(fdl_project_dir):
    """Dotted key targets.default.url creates nested TOML sections."""
    path = fdl_project_dir / "fdl.toml"
    set_value("targets.default.url", "s3://bucket", path)
    result = get_all(path)
    assert result["targets.default.url"] == "s3://bucket"


def test_set_value_four_level_key_is_rejected(fdl_project_dir):
    """Keys deeper than 3 levels are rejected."""
    with pytest.raises(ValueError, match="Key too deep"):
        set_value("a.b.c.d", "x", fdl_project_dir / "fdl.toml")


def test_set_value_preserves_special_characters(fdl_project_dir):
    """Quotes and backslashes in values survive a set → get round-trip.

    Important for S3 endpoints and Windows paths.
    """
    path = fdl_project_dir / "fdl.toml"
    set_value("quote", 'say "hello"', path)
    set_value("backslash", "C:\\Users\\data", path)
    result = get_all(path)
    assert result["quote"] == 'say "hello"'
    assert result["backslash"] == "C:\\Users\\data"


def test_get_all_includes_toplevel_scalars(fdl_project_dir):
    """get_all returns top-level keys like name and catalog, not just sections."""
    path = fdl_project_dir / "fdl.toml"
    set_value("name", "myds", path)
    set_value("catalog", "duckdb", path)
    set_value("targets.default.url", "s3://bucket", path)
    result = get_all(path)
    assert result["name"] == "myds"
    assert result["catalog"] == "duckdb"
    assert result["targets.default.url"] == "s3://bucket"


# ---------------------------------------------------------------------------
# datasource_name
# Spec: reads name from fdl.toml. Raises if missing (no fallback).
# ---------------------------------------------------------------------------


def test_datasource_name_reads_from_toml(fdl_project_dir):
    """Returns the name field from fdl.toml."""
    set_value("name", "myds", fdl_project_dir / "fdl.toml")
    assert datasource_name(fdl_project_dir) == "myds"


def test_datasource_name_without_toml_raises(fdl_project_dir):
    """Raises FileNotFoundError when fdl.toml doesn't exist."""
    with pytest.raises(FileNotFoundError):
        datasource_name(fdl_project_dir)


# ---------------------------------------------------------------------------
# storage / data_path / catalog_path
# Spec: FDL_* env vars with defaults. catalog_path auto-detects sqlite/duckdb.
# ---------------------------------------------------------------------------


def test_storage_defaults_to_fdl_dir(fdl_project_dir):
    """Without FDL_STORAGE env var, defaults to .fdl."""
    assert storage() == ".fdl"


def test_storage_respects_env_var(fdl_project_dir, monkeypatch):
    """FDL_STORAGE env var overrides the default."""
    monkeypatch.setenv("FDL_STORAGE", "/custom/storage")
    assert storage() == "/custom/storage"


def test_data_path_derives_from_storage(fdl_project_dir):
    """FDL_DATA_PATH = {storage}/ducklake.duckdb.files/ by default."""
    assert data_path() == ".fdl/ducklake.duckdb.files/"


def test_data_path_respects_env_var(fdl_project_dir, monkeypatch):
    """FDL_DATA_PATH env var overrides the derived path."""
    monkeypatch.setenv("FDL_DATA_PATH", "/custom/data")
    assert data_path() == "/custom/data"


def test_catalog_path_returns_duckdb(fdl_project_dir):
    """Returns .fdl/ducklake.duckdb when the DuckDB catalog exists."""
    (fdl_project_dir / ".fdl").mkdir()
    (fdl_project_dir / ".fdl" / "ducklake.duckdb").touch()
    assert catalog_path() == ".fdl/ducklake.duckdb"


def test_catalog_path_prefers_sqlite_when_both_exist(fdl_project_dir):
    """When both catalogs exist, SQLite is preferred for dlt compatibility."""
    (fdl_project_dir / ".fdl").mkdir()
    (fdl_project_dir / ".fdl" / "ducklake.duckdb").touch()
    (fdl_project_dir / ".fdl" / "ducklake.sqlite").touch()
    assert catalog_path() == ".fdl/ducklake.sqlite"


def test_catalog_path_respects_env_var(fdl_project_dir, monkeypatch):
    """FDL_CATALOG env var overrides auto-detection."""
    monkeypatch.setenv("FDL_CATALOG", "/custom/catalog.db")
    assert catalog_path() == "/custom/catalog.db"


# ---------------------------------------------------------------------------
# resolve_target
# Spec: all commands require a registered target name. Direct URLs are rejected.
#        ${VAR} and ~ are expanded.
# ---------------------------------------------------------------------------


def test_resolve_target_returns_url(fdl_project_dir):
    """Resolves a registered target name to its URL from fdl.toml."""
    _setup_target(fdl_project_dir, url="s3://my-bucket")
    assert resolve_target("default", fdl_project_dir) == "s3://my-bucket"


def test_resolve_target_unknown_name_raises(fdl_project_dir):
    """Raises ValueError for an unregistered target name."""
    set_value("name", "test", fdl_project_dir / "fdl.toml")
    with pytest.raises(ValueError, match="not found"):
        resolve_target("nonexistent", fdl_project_dir)


def test_resolve_target_rejects_direct_s3_url(fdl_project_dir):
    """Direct S3 URLs are rejected — must use a registered target name."""
    with pytest.raises(ValueError, match="looks like a URL"):
        resolve_target("s3://bucket", fdl_project_dir)


def test_resolve_target_rejects_absolute_path(fdl_project_dir):
    """Absolute paths are rejected — must use a registered target name."""
    with pytest.raises(ValueError, match="looks like a URL"):
        resolve_target("/some/path", fdl_project_dir)


def test_resolve_target_rejects_relative_path(fdl_project_dir):
    """Relative paths are rejected — must use a registered target name."""
    with pytest.raises(ValueError, match="looks like a URL"):
        resolve_target("./path", fdl_project_dir)


def test_resolve_target_expands_env_vars(fdl_project_dir, monkeypatch):
    """${VAR} in target URL is expanded from the environment."""
    monkeypatch.setenv("MY_BUCKET", "s3://real-bucket")
    _setup_target(fdl_project_dir, url="${MY_BUCKET}")
    assert resolve_target("default", fdl_project_dir) == "s3://real-bucket"


def test_resolve_target_expands_tilde(fdl_project_dir):
    """~ in local target URL is expanded to the home directory."""
    _setup_target(fdl_project_dir, url="~/data/fdl")
    result = resolve_target("default", fdl_project_dir)
    assert result.startswith(str(Path.home()))
    assert result.endswith("data/fdl")


# ---------------------------------------------------------------------------
# target_s3_config
# Spec: builds S3Config from fdl.toml target settings with ${VAR} expansion
# ---------------------------------------------------------------------------


def test_s3_config_from_toml(fdl_project_dir):
    """Builds S3Config with correct bucket, endpoint, and credentials."""
    _setup_target(
        fdl_project_dir,
        url="s3://my-bucket",
        s3_endpoint="https://r2.dev",
        s3_access_key_id="AKID",
        s3_secret_access_key="SECRET",
    )
    s3 = target_s3_config("default", fdl_project_dir)
    assert s3.bucket == "my-bucket"
    assert s3.endpoint == "https://r2.dev"
    assert s3.access_key_id == "AKID"
    assert s3.secret_access_key == "SECRET"


def test_s3_config_expands_env_vars(fdl_project_dir, monkeypatch):
    """${VAR} in S3 credentials is expanded from the environment."""
    monkeypatch.setenv("EP", "https://r2.dev")
    _setup_target(fdl_project_dir, url="s3://bucket", s3_endpoint="${EP}")
    assert target_s3_config("default", fdl_project_dir).endpoint == "https://r2.dev"


def test_s3_config_rejects_non_s3_target(fdl_project_dir):
    """Raises ValueError for a local target — S3Config is S3-only."""
    _setup_target(fdl_project_dir, url="~/local/path")
    with pytest.raises(ValueError, match="not an S3 target"):
        target_s3_config("default", fdl_project_dir)


# ---------------------------------------------------------------------------
# target_public_url / ducklake_url
# Spec: public_url is the base URL for HTTP access to datasets
# ---------------------------------------------------------------------------


def test_public_url_returns_configured_value(fdl_project_dir):
    """Returns the public_url from fdl.toml."""
    _setup_target(fdl_project_dir, public_url="http://localhost:4001")
    assert target_public_url("default", fdl_project_dir) == "http://localhost:4001"


def test_public_url_returns_none_when_not_set(fdl_project_dir):
    """Returns None when public_url is not configured for the target."""
    _setup_target(fdl_project_dir)
    assert target_public_url("default", fdl_project_dir) is None


def test_ducklake_url_constructs_full_catalog_url(fdl_project_dir):
    """Constructs {public_url}/{datasource}/ducklake.duckdb."""
    _setup_target(fdl_project_dir, public_url="http://localhost:4001")
    assert ducklake_url("my_ds", "default", fdl_project_dir) == \
        "http://localhost:4001/my_ds/ducklake.duckdb"


def test_ducklake_url_raises_without_public_url(fdl_project_dir):
    """Raises KeyError when public_url is not configured."""
    _setup_target(fdl_project_dir)
    with pytest.raises(KeyError, match="public_url not configured"):
        ducklake_url("my_ds", "default", fdl_project_dir)


# ---------------------------------------------------------------------------
# fdl_env_dict
# Spec: all FDL_* env vars injected by fdl run.
#        Local targets get STORAGE/DATA_PATH/CATALOG.
#        S3 targets also get FDL_S3_*.
# ---------------------------------------------------------------------------


def test_env_dict_contains_storage_data_path_catalog(fdl_project_dir):
    """Local target produces correct FDL_STORAGE, FDL_DATA_PATH, FDL_CATALOG.

    Spec: FDL_STORAGE = {target_url}/{datasource}
          FDL_DATA_PATH = {FDL_STORAGE}/ducklake.duckdb.files/
          FDL_CATALOG = .fdl/{target}/ducklake.duckdb (auto-detected)
    """
    _setup_target(fdl_project_dir, url=str(fdl_project_dir / "storage"))
    storage_val = str(fdl_project_dir / "storage" / "ds")
    env = fdl_env_dict(target_name="default", storage_override=storage_val)
    assert env["FDL_STORAGE"] == storage_val
    assert env["FDL_DATA_PATH"] == f"{storage_val}/ducklake.duckdb.files/"
    assert env["FDL_CATALOG"] == ".fdl/default/ducklake.duckdb"


def test_env_dict_includes_s3_vars_for_s3_target(fdl_project_dir):
    """S3 target additionally includes FDL_S3_* credential keys."""
    _setup_target(
        fdl_project_dir,
        url="s3://bucket",
        s3_endpoint="https://r2.dev",
        s3_access_key_id="AKID",
        s3_secret_access_key="SECRET",
    )
    env = fdl_env_dict(target_name="default", storage_override="s3://bucket/ds")
    assert env["FDL_S3_ENDPOINT"] == "https://r2.dev"
    assert env["FDL_S3_ENDPOINT_HOST"] == "r2.dev"
    assert env["FDL_S3_ACCESS_KEY_ID"] == "AKID"
    assert env["FDL_S3_SECRET_ACCESS_KEY"] == "SECRET"


def test_env_dict_omits_s3_vars_for_local_target(fdl_project_dir):
    """Local target does not include FDL_S3_* keys."""
    _setup_target(fdl_project_dir, url=str(fdl_project_dir / "storage"))
    env = fdl_env_dict(
        target_name="default",
        storage_override=str(fdl_project_dir / "storage" / "ds"),
    )
    assert not any(k.startswith("FDL_S3_") for k in env)
