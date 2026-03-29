"""Integration tests for fdl checkpoint.

Uses DuckLake CHECKPOINT statement for maintenance.
"""

import json

from typer.testing import CliRunner

from fdl.cli import app


def test_checkpoint_succeeds(fdl_project_dir):
    """fdl checkpoint runs DuckLake CHECKPOINT on the catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (1)"])

    result = cli.invoke(app, ["checkpoint", "default"])
    assert result.exit_code == 0, result.output
    assert "Checkpoint complete" in result.output


def test_active_files_are_preserved(fdl_project_dir):
    """Checkpoint does not delete files still referenced by the catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (1)"])

    data_dir = storage / "test_ds" / "ducklake.duckdb.files"
    files_before = set(data_dir.rglob("*.parquet"))
    assert len(files_before) > 0

    cli.invoke(app, ["checkpoint", "default"])
    assert set(data_dir.rglob("*.parquet")) == files_before


def test_stale_catalog_is_rejected(fdl_project_dir):
    """Checkpoint fails when local catalog is older than remote.

    Running maintenance with a stale catalog could delete active files
    that were added by someone else's push.
    """
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["push", "default"])

    remote_meta = storage / "test_ds" / ".fdl" / "meta.json"
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(app, ["checkpoint", "default"])
    assert result.exit_code != 0
    assert "fdl pull" in result.output.lower()


def test_force_skips_freshness_check(fdl_project_dir):
    """--force allows checkpoint even with a stale catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["push", "default"])

    remote_meta = storage / "test_ds" / ".fdl" / "meta.json"
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(app, ["checkpoint", "default", "--force"])
    assert result.exit_code == 0, result.output


def test_without_init_fails(fdl_project_dir):
    """Checkpoint fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["checkpoint", "default"])
    assert result.exit_code != 0


def test_missing_catalog_fails(fdl_project_dir):
    """Checkpoint fails when catalog doesn't exist."""
    from fdl.config import set_value

    set_value("name", "test_ds", fdl_project_dir / "fdl.toml")
    set_value("targets.default.url", str(fdl_project_dir / "storage"), fdl_project_dir / "fdl.toml")
    (fdl_project_dir / ".fdl").mkdir()

    result = CliRunner().invoke(app, ["checkpoint", "default"])
    assert result.exit_code != 0
