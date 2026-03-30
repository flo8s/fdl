"""Integration tests for fdl sql.

Spec (docs/reference/cli.md#sql):
  fdl sql TARGET QUERY [--force]
  - Executes SQL against the DuckLake catalog
  - Each invocation opens a new connection (no cross-invocation transactions)
  - Writes directly to target storage
  - Checks for stale catalog before execution (skip with --force)
"""

import json
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


def test_stale_catalog_is_rejected(fdl_project_dir: Path):
    """sql fails when local catalog is older than remote.

    Running queries with a stale catalog could return incorrect results
    or, for maintenance operations like CHECKPOINT, delete active files
    that were added by another user's push.
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

    result = cli.invoke(app, ["sql", "default", "SELECT 1"])
    assert result.exit_code != 0
    assert "fdl pull" in result.output.lower()


def test_force_skips_freshness_check(fdl_project_dir: Path):
    """--force allows sql execution even with a stale catalog."""
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

    result = cli.invoke(app, ["sql", "default", "--force", "SELECT 1"])
    assert result.exit_code == 0, result.output
