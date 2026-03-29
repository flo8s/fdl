"""Tests for fdl/__init__.py.

ducklake_data_path: DATA_PATH is derived by appending ".files/" to the catalog URL.
  Getting this wrong means data files can't be found.

default_target_url: The default storage location shown in fdl init prompt.
  Users typically accept the default, so this directly determines where data is stored.
  Must follow XDG Base Directory Specification.
"""

from pathlib import Path

from fdl import default_target_url, ducklake_data_path


def test_ducklake_data_path():
    """DATA_PATH = catalog URL + '.files/' (spec: data file location convention)."""
    assert ducklake_data_path("foo/bar") == "foo/bar.files/"


def test_ducklake_data_path_s3():
    """Same convention applies to S3 URLs."""
    assert (
        ducklake_data_path("s3://bucket/ds/ducklake.duckdb")
        == "s3://bucket/ds/ducklake.duckdb.files/"
    )


def test_default_target_url_no_xdg(monkeypatch):
    """Most common case: XDG_DATA_HOME unset, falls back to ~/.local/share/fdl."""
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert default_target_url() == "~/.local/share/fdl"


def test_default_target_url_with_xdg(monkeypatch):
    """When XDG_DATA_HOME is set under home, returns ~/relative/fdl form."""
    home = str(Path.home())
    monkeypatch.setenv("XDG_DATA_HOME", f"{home}/custom/data")
    assert default_target_url() == "~/custom/data/fdl"


def test_default_target_url_outside_home(monkeypatch):
    """When XDG_DATA_HOME is outside home directory, returns absolute path."""
    monkeypatch.setenv("XDG_DATA_HOME", "/opt/data")
    assert default_target_url() == "/opt/data/fdl"
