"""Shared fixtures for all tests."""

from pathlib import Path

import pytest


@pytest.fixture
def fdl_project_dir(tmp_path, monkeypatch) -> Path:
    """Isolate Path.cwd() to tmp_path and clear FDL_* env vars."""
    monkeypatch.chdir(tmp_path)
    for key in ["FDL_STORAGE", "FDL_DATA_PATH", "FDL_CATALOG", "XDG_DATA_HOME"]:
        monkeypatch.delenv(key, raising=False)
    return tmp_path
