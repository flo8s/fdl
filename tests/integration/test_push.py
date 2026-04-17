"""Integration tests for fdl push (local targets).

Spec (docs/reference/cli.md#push):
  fdl push TARGET [--force]
  - Pushes ducklake.duckdb and fdl.toml to the target
  - Data files are NOT included
  - SQLite catalogs are automatically converted to DuckDB during push
  - Conflict detection applies to S3 targets only (see test_s3.py);
    local targets are assumed single-user
"""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_copies_catalog_and_toml_to_target(fdl_project_dir: Path):
    """fdl push copies ducklake.duckdb and fdl.toml to the target directory."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output
    assert (storage / "test_ds" / "ducklake.duckdb").exists()
    assert (storage / "test_ds" / "fdl.toml").exists()


def test_sqlite_is_converted_to_duckdb_on_push(fdl_project_dir: Path):
    """fdl push with SQLite catalog converts it to DuckDB at the target."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
        "--sqlite",
    ])

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output
    assert (storage / "test_ds" / "ducklake.duckdb").exists()


def test_second_push_after_first_succeeds(fdl_project_dir: Path):
    """Consecutive pushes from the same user succeed."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    cli.invoke(app, ["push", "default"])
    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output


def test_without_init_fails(fdl_project_dir: Path):
    """fdl push fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code != 0


def test_unknown_target_fails(fdl_project_dir: Path):
    """fdl push with an unregistered target name fails."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["push", "nonexistent"])
    assert result.exit_code != 0
