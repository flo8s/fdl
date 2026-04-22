"""Integration tests for fdl sql.

Spec (docs/reference/cli.md#sql):
  fdl sql TARGET QUERY [--force]
  - Executes SQL against the DuckLake catalog
  - Each invocation opens a new connection (no cross-invocation transactions)
  - Writes directly to target storage
  - Checks for stale catalog before execution on S3 targets
    (skip with --force). Local targets skip the check.
"""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_table_data_persists_across_invocations(fdl_project_dir: Path):
    """Data created in one fdl sql call is visible in subsequent calls."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    cli.invoke(app, ["sql", "default", "CREATE TABLE cities (name VARCHAR, pop INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO cities VALUES ('Tokyo', 14000000)"])
    result = cli.invoke(app, ["sql", "default", "SELECT name, pop FROM cities"])
    assert result.exit_code == 0
    assert "Tokyo" in result.output
    assert "14000000" in result.output


def test_invalid_sql_fails(fdl_project_dir: Path):
    """Invalid SQL propagates a DuckDB error."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["sql", "default", "SELECT * FROM nonexistent_table"])
    assert result.exit_code != 0


def test_sql_errors_when_target_has_no_catalog(fdl_project_dir: Path):
    """fdl sql on a target that has never been init'd/pulled surfaces a helpful error."""
    from fdl.config import set_value

    # Bootstrap just fdl.toml (no .fdl/ yet) so datasource resolution works.
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    set_value("targets.local.url", str(fdl_project_dir / "other"), fdl_project_dir / "fdl.toml")
    set_value("targets.local.public_url", "http://localhost:4001", fdl_project_dir / "fdl.toml")

    result = cli.invoke(app, ["sql", "local", "SELECT 1"])
    assert result.exit_code != 0
    assert "fdl init" in result.output
    assert "fdl pull" in result.output


# Stale-catalog detection for sql is only performed against S3 targets;
# see tests/integration/test_s3.py::test_sql_rejects_stale_catalog.
