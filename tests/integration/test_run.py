"""Integration tests for run_command (run + implicit publish)."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

import duckdb
import pytest

from fdl import DUCKLAKE_FILE, FDL_DIR
from fdl.config import CatalogSpec
from fdl.ducklake import build_attach_sql, init_ducklake
from fdl.run import run_command


def _setup(
    project_dir: Path,
    *,
    publishes: dict[str, str] | None = None,
    command: str | None = None,
) -> Path:
    sqlite_path = project_dir / FDL_DIR / "ducklake.sqlite"
    data_dir = project_dir / FDL_DIR / "data"
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        'name = "ds"',
    ]
    if command:
        lines.append(f'command = "{command}"')
    lines += [
        "",
        "[metadata]",
        f'url = "sqlite:///{sqlite_path.resolve()}"',
        "",
        "[data]",
        f'url = "{data_dir.resolve()}"',
    ]
    for name, url in (publishes or {}).items():
        lines += [
            "",
            f"[publishes.{name}]",
            f'url = "{url}"',
        ]
    (project_dir / "fdl.toml").write_text("\n".join(lines) + "\n")

    spec = CatalogSpec(
        scheme="sqlite",
        raw=f"sqlite:///{sqlite_path}",
        path=str(sqlite_path),
    )
    init_ducklake(spec, str(data_dir), "ds")
    return sqlite_path


class TestRunV11:
    def test_run_no_publishes_skips_publish(self, fdl_project_dir):
        _setup(fdl_project_dir)
        rc = run_command(
            publish_name=None,
            cmd=[sys.executable, "-c", "print('hello')"],
            project_dir=fdl_project_dir,
        )
        assert rc == 0

    def test_run_single_publish_publishes_implicitly(
        self, fdl_project_dir, tmp_path,
    ):
        dest = tmp_path / "pub"
        _setup(fdl_project_dir, publishes={"default": str(dest)})
        rc = run_command(
            publish_name=None,
            cmd=[sys.executable, "-c", "print('ok')"],
            project_dir=fdl_project_dir,
        )
        assert rc == 0
        assert (dest / "fdl.toml").exists()
        assert (dest / DUCKLAKE_FILE).exists()

    def test_multiple_publishes_without_name_errors(
        self, fdl_project_dir, tmp_path,
    ):
        _setup(
            fdl_project_dir,
            publishes={
                "a": str(tmp_path / "a"),
                "b": str(tmp_path / "b"),
            },
        )
        with pytest.raises(ValueError, match="Multiple"):
            run_command(
                publish_name=None,
                cmd=[sys.executable, "-c", "print('x')"],
                project_dir=fdl_project_dir,
            )

    def test_explicit_publish_name_selects_one(
        self, fdl_project_dir, tmp_path,
    ):
        dest_a = tmp_path / "a"
        dest_b = tmp_path / "b"
        _setup(
            fdl_project_dir,
            publishes={"a": str(dest_a), "b": str(dest_b)},
        )
        rc = run_command(
            publish_name="b",
            cmd=[sys.executable, "-c", "print('picked b')"],
            project_dir=fdl_project_dir,
        )
        assert rc == 0
        assert (dest_b / DUCKLAKE_FILE).exists()
        assert not (dest_a / DUCKLAKE_FILE).exists()

    def test_non_zero_exit_skips_publish(self, fdl_project_dir, tmp_path):
        dest = tmp_path / "pub"
        _setup(fdl_project_dir, publishes={"default": str(dest)})
        rc = run_command(
            publish_name=None,
            cmd=[sys.executable, "-c", "import sys; sys.exit(3)"],
            project_dir=fdl_project_dir,
        )
        assert rc == 3
        assert not (dest / DUCKLAKE_FILE).exists()

    def test_command_from_fdl_toml(self, fdl_project_dir, tmp_path):
        dest = tmp_path / "pub"
        _setup(
            fdl_project_dir,
            publishes={"default": str(dest)},
            command=f"{sys.executable} -c pass",
        )
        rc = run_command(
            publish_name=None, cmd=None, project_dir=fdl_project_dir,
        )
        assert rc == 0
        assert (dest / DUCKLAKE_FILE).exists()

    def test_env_propagates_fdl_catalog_url(self, fdl_project_dir, tmp_path):
        sqlite_path = _setup(fdl_project_dir)
        script = (
            "import os, sys; "
            "sys.stdout.write(os.environ.get('FDL_CATALOG_URL', '<unset>'))"
        )
        # Redirect the subprocess's stdout to a file to observe the env var.
        out = fdl_project_dir / "out.txt"
        rc = run_command(
            publish_name=None,
            cmd=[
                sys.executable, "-c",
                f"import sys; open({str(out)!r}, 'w').write("
                "__import__('os').environ.get('FDL_CATALOG_URL', '<unset>'))",
            ],
            project_dir=fdl_project_dir,
        )
        assert rc == 0
        assert out.read_text().startswith("sqlite:///")
        assert str(sqlite_path.resolve()) in out.read_text()
