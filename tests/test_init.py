"""Tests for init_project (sqlite path; postgres is integration-only)."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from fdl import FDL_DIR
from fdl.init_project import init_project


def test_defaults_create_sqlite_project(fdl_project_dir):
    init_project("myds", project_dir=fdl_project_dir)

    toml = tomllib.loads((fdl_project_dir / "fdl.toml").read_text())
    assert toml["name"] == "myds"
    assert toml["metadata"]["url"].startswith("sqlite:///")
    assert toml["data"]["url"].endswith(".fdl/data")
    assert "publishes" not in toml

    # Live sqlite catalog materialized.
    sqlite_file = fdl_project_dir / FDL_DIR / "ducklake.sqlite"
    assert sqlite_file.exists()


def test_with_publish_url(fdl_project_dir):
    init_project(
        "ds",
        publish_url="/tmp/some/path",
        publish_name="public",
        project_dir=fdl_project_dir,
    )
    toml = tomllib.loads((fdl_project_dir / "fdl.toml").read_text())
    assert toml["publishes"]["public"]["url"] == "/tmp/some/path"


def test_existing_fdl_toml_errors(fdl_project_dir):
    (fdl_project_dir / "fdl.toml").write_text('name = "existing"\n')
    with pytest.raises(FileExistsError):
        init_project("ds", project_dir=fdl_project_dir)


def test_invalid_name_errors(fdl_project_dir):
    with pytest.raises(ValueError, match="valid SQL identifier"):
        init_project("1-bad", project_dir=fdl_project_dir)


def test_rollback_on_failure(fdl_project_dir, monkeypatch):
    """If catalog init blows up, fdl.toml and .fdl/ are cleaned up."""
    def _boom(*a, **k):
        raise RuntimeError("kaboom")

    monkeypatch.setattr("fdl.ducklake.init_ducklake", _boom)
    with pytest.raises(RuntimeError, match="kaboom"):
        init_project("ds", project_dir=fdl_project_dir)
    assert not (fdl_project_dir / "fdl.toml").exists()
    assert not (fdl_project_dir / FDL_DIR).exists()


def test_var_expansion(fdl_project_dir, monkeypatch):
    monkeypatch.setenv("MYDIR", str(fdl_project_dir / "custom"))
    init_project(
        "ds",
        metadata_url=f"sqlite:///{(fdl_project_dir / 'custom' / 'x.sqlite').as_posix()}",
        data_url="${MYDIR}/data",
        project_dir=fdl_project_dir,
    )
    toml = tomllib.loads((fdl_project_dir / "fdl.toml").read_text())
    # Written verbatim, pre-expansion — so user can version-control it safely.
    assert "${MYDIR}/data" in toml["data"]["url"]
