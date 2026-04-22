"""Shared fixtures for all tests."""

from pathlib import Path

import pytest


@pytest.fixture
def fdl_project_dir(tmp_path, monkeypatch) -> Path:
    """Isolate Path.cwd() to tmp_path and clear FDL_* env vars."""
    monkeypatch.chdir(tmp_path)
    for key in [
        "FDL_CATALOG_URL",
        "FDL_CATALOG_PATH",
        "FDL_DATA_URL",
        "FDL_DATA_BUCKET",
        "FDL_DATA_PREFIX",
        "XDG_DATA_HOME",
    ]:
        monkeypatch.delenv(key, raising=False)
    return tmp_path
