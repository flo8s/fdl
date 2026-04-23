"""Tests for the public Python API (fdl.init, .pull, .publish, .run, .connect)."""

from __future__ import annotations

from pathlib import Path

import pytest

import fdl


def _init_with_publish(project_dir: Path, *, publish_to: Path) -> None:
    """Initialize a v0.11 project that publishes to a local directory."""
    publish_to.mkdir(parents=True, exist_ok=True)
    fdl.init("mydata", publish_url=str(publish_to), project_dir=project_dir)


class TestRoundTrip:
    def test_init_publish_pull(self, fdl_project_dir, tmp_path_factory):
        """init → connect+write → publish → pull (mirror project) → read."""
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        with fdl.connect(project_dir=fdl_project_dir) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (1), (2), (3)")

        fdl.publish(project_dir=fdl_project_dir)

        from fdl import FDL_DIR

        mirror_root = tmp_path_factory.mktemp("mirror")
        mirror_sqlite = mirror_root / FDL_DIR / "ducklake.sqlite"
        src_data = (fdl_project_dir / FDL_DIR / "data").resolve()
        (mirror_root / "fdl.toml").write_text(
            f'name = "mydata"\n\n'
            f"[metadata]\n"
            f'url = "sqlite:///{mirror_sqlite.resolve()}"\n\n'
            f"[data]\n"
            f'url = "{src_data}"\n\n'
            f"[publishes.default]\n"
            f'url = "{publish_to}"\n'
        )

        fdl.pull(project_dir=mirror_root)

        with fdl.connect(project_dir=mirror_root) as conn:
            rows = conn.execute("SELECT x FROM t ORDER BY x").fetchall()
        assert rows == [(1,), (2,), (3,)]


class TestConnect:
    def test_walks_up_from_subdirectory(
        self, fdl_project_dir, tmp_path_factory, monkeypatch,
    ):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        subdir = fdl_project_dir / "pipelines" / "etl"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        with fdl.connect() as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t VALUES (42)")
            rows = conn.execute("SELECT x FROM t").fetchall()
        assert rows == [(42,)]

    def test_explicit_project_dir_from_unrelated_cwd(
        self, fdl_project_dir, tmp_path_factory, monkeypatch,
    ):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        unrelated = tmp_path_factory.mktemp("unrelated")
        monkeypatch.chdir(unrelated)

        with fdl.connect(project_dir=fdl_project_dir) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")

    def test_closes_on_exit(self, fdl_project_dir, tmp_path_factory):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        with fdl.connect(project_dir=fdl_project_dir) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
        with pytest.raises(Exception):
            conn.execute("SELECT 1")


class TestRun:
    def test_returns_nonzero_exit_code(
        self, fdl_project_dir, tmp_path_factory,
    ):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        import sys

        rc = fdl.run(
            None,
            [sys.executable, "-c", "import sys; sys.exit(7)"],
            project_dir=fdl_project_dir,
        )
        assert rc == 7

    def test_nonzero_exit_skips_publish(
        self, fdl_project_dir, tmp_path_factory,
    ):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)

        import sys

        rc = fdl.run(
            None,
            [sys.executable, "-c", "import sys; sys.exit(3)"],
            project_dir=fdl_project_dir,
        )
        assert rc == 3
        # Nothing was published
        assert not (publish_to / "ducklake.duckdb").exists()

    def test_without_command_raises_when_missing(
        self, fdl_project_dir, tmp_path_factory,
    ):
        publish_to = tmp_path_factory.mktemp("remote")
        _init_with_publish(fdl_project_dir, publish_to=publish_to)
        with pytest.raises(ValueError, match="No command"):
            fdl.run(None, None, project_dir=fdl_project_dir)


class TestInit:
    def test_rejects_invalid_name(self, fdl_project_dir):
        with pytest.raises(ValueError, match="SQL identifier"):
            fdl.init("my-data", project_dir=fdl_project_dir)

    def test_rejects_existing_config(self, fdl_project_dir):
        (fdl_project_dir / "fdl.toml").write_text('name = "existing"\n')
        with pytest.raises(FileExistsError):
            fdl.init("mydata", project_dir=fdl_project_dir)

    def test_rollback_on_failure(
        self, fdl_project_dir, monkeypatch,
    ):
        from fdl import FDL_DIR
        from fdl.config import PROJECT_CONFIG

        def _boom(*a, **k):
            raise RuntimeError("simulated")

        monkeypatch.setattr("fdl.ducklake.init_ducklake", _boom)
        with pytest.raises(RuntimeError, match="simulated"):
            fdl.init("mydata", project_dir=fdl_project_dir)

        assert not (fdl_project_dir / PROJECT_CONFIG).exists()
        assert not (fdl_project_dir / FDL_DIR).exists()


def test_public_api_surface():
    """__all__ exposes exactly the documented public names."""
    expected = {
        # Constants
        "DUCKLAKE_FILE",
        "DUCKLAKE_SQLITE",
        "FDL_DIR",
        "META_JSON",
        # Python API
        "connect",
        "init",
        "publish",
        "pull",
        "run",
    }
    assert set(fdl.__all__) == expected
    for name in expected:
        assert hasattr(fdl, name), name
