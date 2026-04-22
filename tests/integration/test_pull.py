"""Integration tests for fdl pull (local targets).

Spec (docs/reference/cli.md#pull):
  fdl pull TARGET
  - Downloads catalog from target
  - Requires prior fdl init
  - For S3 targets, records the remote catalog ETag into
    ``.fdl/<target>/meta.json`` (see test_s3.py). Local targets do not
    maintain conflict-detection state.
"""

from typer.testing import CliRunner

from fdl.cli import app


def test_pulled_catalog_is_usable(fdl_project_dir):
    """Data created before push is queryable after pull."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    # Create data and push
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (42)"])
    cli.invoke(app, ["push", "default"])

    # Delete local catalog
    (fdl_project_dir / ".fdl" / "default" / "ducklake.sqlite").unlink()

    # Pull and verify data survived
    cli.invoke(app, ["pull", "default"])
    result = cli.invoke(app, ["sql", "default", "SELECT x FROM t"])
    assert result.exit_code == 0
    assert "42" in result.output


def test_pull_then_push_succeeds(fdl_project_dir):
    """The pull → push workflow succeeds."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    cli.invoke(app, ["push", "default"])
    cli.invoke(app, ["pull", "default"])

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output


def test_without_init_fails(fdl_project_dir):
    """fdl pull fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["pull", "default"])
    assert result.exit_code != 0


def test_unknown_target_fails(fdl_project_dir):
    """fdl pull with an unregistered target name fails."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["pull", "nonexistent"])
    assert result.exit_code != 0


def test_pull_from_empty_target_does_not_restore_catalog(fdl_project_dir):
    """fdl pull from a target that was never pushed to does not create a catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    # Delete local catalog without pushing first
    (fdl_project_dir / ".fdl" / "default" / "ducklake.sqlite").unlink()

    # Pull from empty target
    cli.invoke(app, ["pull", "default"])
    assert not (fdl_project_dir / ".fdl" / "default" / "ducklake.sqlite").exists()
    assert not (fdl_project_dir / ".fdl" / "default" / "ducklake.duckdb").exists()


def test_pull_converts_remote_duckdb_to_local_sqlite(fdl_project_dir, tmp_path_factory):
    """Pull into a freshly-cloned project produces a SQLite-only local layout."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (7)"])
    cli.invoke(app, ["push", "default"])

    # Second project that only knows the shared target dir.
    import shutil

    other = tmp_path_factory.mktemp("other")
    shutil.copy2(fdl_project_dir / "fdl.toml", other / "fdl.toml")

    import os
    cwd = os.getcwd()
    try:
        os.chdir(other)
        result = cli.invoke(app, ["pull", "default"])
    finally:
        os.chdir(cwd)

    assert result.exit_code == 0, result.output
    assert (other / ".fdl" / "default" / "ducklake.sqlite").exists()
    assert not (other / ".fdl" / "default" / "ducklake.duckdb").exists()
