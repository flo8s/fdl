"""Integration tests for fdl sql.

Spec (docs/reference/cli.md#sql):
  fdl sql TARGET QUERY
  - Executes SQL against the DuckLake catalog
  - Each invocation opens a new connection (no cross-invocation transactions)
  - Writes directly to target storage
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
