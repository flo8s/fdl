"""Tests for fdl.config.find_project_dir."""

from __future__ import annotations

from pathlib import Path

import pytest

from fdl.config import find_project_dir


def test_finds_fdl_toml_in_cwd(fdl_project_dir: Path) -> None:
    """Returns the directory itself when fdl.toml is in cwd."""
    (fdl_project_dir / "fdl.toml").write_text('name = "x"\n')

    assert find_project_dir() == fdl_project_dir.resolve()


def test_walks_up_directory_tree(fdl_project_dir: Path, monkeypatch) -> None:
    """Returns the nearest ancestor that contains fdl.toml."""
    (fdl_project_dir / "fdl.toml").write_text('name = "x"\n')
    nested = fdl_project_dir / "a" / "b" / "c"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert find_project_dir() == fdl_project_dir.resolve()


def test_prefers_closest_fdl_toml(fdl_project_dir: Path, monkeypatch) -> None:
    """When multiple ancestors have fdl.toml, returns the nearest one."""
    (fdl_project_dir / "fdl.toml").write_text('name = "outer"\n')
    inner = fdl_project_dir / "inner"
    inner.mkdir()
    (inner / "fdl.toml").write_text('name = "inner"\n')
    leaf = inner / "leaf"
    leaf.mkdir()
    monkeypatch.chdir(leaf)

    assert find_project_dir() == inner.resolve()


def test_raises_when_not_found(fdl_project_dir: Path) -> None:
    """Raises FileNotFoundError when fdl.toml is nowhere above cwd."""
    with pytest.raises(FileNotFoundError, match="fdl.toml"):
        find_project_dir()


def test_accepts_explicit_start(fdl_project_dir: Path) -> None:
    """The start argument overrides cwd-based search."""
    (fdl_project_dir / "fdl.toml").write_text('name = "x"\n')
    elsewhere = fdl_project_dir / "elsewhere"
    elsewhere.mkdir()

    assert find_project_dir(elsewhere) == fdl_project_dir.resolve()
