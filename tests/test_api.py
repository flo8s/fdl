"""Tests for the public Python API (fdl.init, .pull, .push, .run, .sync, .connect)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import fdl


def _init_local_project(project_dir: Path, *, remote: Path) -> None:
    """Initialize a project that pushes/pulls to a local directory."""
    remote.mkdir(parents=True, exist_ok=True)
    fdl.init("mydata", target_url=str(remote), project_dir=project_dir)


def test_roundtrip_local(fdl_project_dir: Path, tmp_path_factory) -> None:
    """init -> connect/write -> push -> wipe -> pull -> connect/read roundtrip."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    with fdl.connect("default") as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1), (2), (3)")

    fdl.push("default")

    # Wipe local catalog to force a real pull
    shutil.rmtree(fdl_project_dir / ".fdl" / "default")

    fdl.pull("default")

    with fdl.connect("default") as conn:
        rows = conn.execute("SELECT x FROM t ORDER BY x").fetchall()
    assert rows == [(1,), (2,), (3,)]


def test_connect_walks_up_from_subdirectory(
    fdl_project_dir: Path, tmp_path_factory, monkeypatch
) -> None:
    """fdl.connect() from a subdirectory finds fdl.toml by walking up."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    subdir = fdl_project_dir / "pipelines" / "etl"
    subdir.mkdir(parents=True)
    monkeypatch.chdir(subdir)

    with fdl.connect("default") as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (42)")
        rows = conn.execute("SELECT x FROM t").fetchall()
    assert rows == [(42,)]


def test_explicit_project_dir_from_unrelated_cwd(
    fdl_project_dir: Path, tmp_path_factory, monkeypatch
) -> None:
    """API calls with explicit project_dir work from any cwd."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    unrelated = tmp_path_factory.mktemp("unrelated")
    monkeypatch.chdir(unrelated)

    with fdl.connect("default", project_dir=fdl_project_dir) as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    fdl.push("default", project_dir=fdl_project_dir)


def test_target_is_required() -> None:
    """All entry points require target as a positional argument."""
    with pytest.raises(TypeError):
        fdl.pull()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        fdl.push()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        fdl.run()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        fdl.sync()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        with fdl.connect():  # type: ignore[call-arg]
            pass


def test_run_returns_nonzero_exit_code(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.run returns the subprocess exit code."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    rc = fdl.run("default", ["python", "-c", "import sys; sys.exit(7)"])
    assert rc == 7


def test_sync_skips_push_when_command_fails(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.sync does not push when the pipeline command fails."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    rc = fdl.sync("default", ["python", "-c", "import sys; sys.exit(3)"])
    assert rc == 3

    # Nothing was pushed to the remote (datasource dir not created)
    assert not (remote / "mydata").exists()


def test_run_uses_command_from_fdl_toml(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.run(target) without a command reads command from fdl.toml."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    from fdl.config import set_value

    set_value("command", "python -c 'import sys; sys.exit(5)'")

    rc = fdl.run("default")
    assert rc == 5


def test_run_without_command_raises_when_missing(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.run(target) without command argument and no fdl.toml command raises."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    with pytest.raises(ValueError, match="No command"):
        fdl.run("default")


def test_connect_closes_on_exit(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.connect closes the underlying connection on context exit."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    with fdl.connect("default") as conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    with pytest.raises(Exception):
        conn.execute("SELECT 1")


def test_init_rejects_invalid_name(fdl_project_dir: Path) -> None:
    """fdl.init raises ValueError for a non-SQL-identifier name."""
    with pytest.raises(ValueError, match="SQL identifier"):
        fdl.init("my-data", project_dir=fdl_project_dir)


def test_init_rejects_existing_config(
    fdl_project_dir: Path, tmp_path_factory
) -> None:
    """fdl.init raises FileExistsError when fdl.toml already exists."""
    remote = tmp_path_factory.mktemp("remote")
    _init_local_project(fdl_project_dir, remote=remote)

    with pytest.raises(FileExistsError):
        fdl.init("mydata", project_dir=fdl_project_dir)


def test_push_raises_push_conflict_error(
    fdl_project_dir: Path, monkeypatch
) -> None:
    """fdl.push raises PushConflictError instead of SystemExit on conflict.

    Uses an S3 target via moto because local targets skip conflict detection.
    """
    import boto3
    from moto import mock_aws

    from fdl.meta import PushConflictError

    bucket = "api-test-bucket"

    def _moto_client_factory(s3):
        return boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=s3.access_key_id,
            aws_secret_access_key=s3.secret_access_key,
        )

    with mock_aws():
        monkeypatch.setattr("fdl.s3.create_s3_client", _moto_client_factory)
        external = boto3.client("s3", region_name="us-east-1")
        external.create_bucket(Bucket=bucket)

        fdl.init(
            "mydata",
            target_url=f"s3://{bucket}",
            project_dir=fdl_project_dir,
        )
        from fdl.config import set_value
        set_value("targets.default.s3_endpoint", "https://s3.us-east-1.amazonaws.com")
        set_value("targets.default.s3_access_key_id", "testing")
        set_value("targets.default.s3_secret_access_key", "testing")

        fdl.push("default", project_dir=fdl_project_dir)

        # Simulate another client overwriting the catalog — this bumps the ETag.
        external.put_object(
            Bucket=bucket,
            Key="mydata/ducklake.duckdb",
            Body=b"external change",
        )

        with pytest.raises(PushConflictError):
            fdl.push("default", project_dir=fdl_project_dir)


def test_init_rollback_on_failure(
    fdl_project_dir: Path, monkeypatch, tmp_path_factory
) -> None:
    """fdl.init rolls back fdl.toml and .fdl/ when init_ducklake fails."""
    from fdl import FDL_DIR
    from fdl.config import PROJECT_CONFIG

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr("fdl.ducklake.init_ducklake", _boom)

    remote = tmp_path_factory.mktemp("remote")
    with pytest.raises(RuntimeError, match="simulated"):
        fdl.init(
            "mydata",
            target_url=str(remote),
            project_dir=fdl_project_dir,
        )

    assert not (fdl_project_dir / PROJECT_CONFIG).exists()
    assert not (fdl_project_dir / FDL_DIR / "default").exists()


def test_public_api_surface() -> None:
    """__all__ exposes exactly the documented public names."""
    expected = {
        # Constants
        "DUCKLAKE_FILE",
        "DUCKLAKE_SQLITE",
        "FDL_DIR",
        "META_JSON",
        # Helpers
        "default_target_url",
        "ducklake_data_path",
        "fdl_target_dir",
        # Python API
        "clone",
        "connect",
        "init",
        "pull",
        "push",
        "run",
        "sync",
    }
    assert set(fdl.__all__) == expected
    for name in expected:
        assert hasattr(fdl, name), name
