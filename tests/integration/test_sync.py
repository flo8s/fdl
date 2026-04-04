"""Integration tests for fdl sync.

Spec (docs/reference/cli.md#sync):
  fdl sync TARGET
  fdl sync TARGET -- COMMAND [ARGS...]
  - Auto-pulls, runs command, pushes catalog
  - Uses command from fdl.toml when no -- COMMAND given
  - Skips push on non-zero exit code
  - --force overrides conflict detection on push
"""

import json
from pathlib import Path

from typer.testing import CliRunner

from fdl import FDL_DIR, META_JSON
from fdl.cli import app
from fdl.config import set_value


def _init_project(fdl_project_dir: Path, cli: CliRunner) -> Path:
    """Initialize a project and return the storage path."""
    storage = fdl_project_dir / "storage"
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    return storage


def test_sync_runs_pull_run_push(fdl_project_dir: Path):
    """fdl sync executes pull, run, and push — catalog and fdl.toml appear at target."""
    cli = CliRunner()
    storage = _init_project(fdl_project_dir, cli)

    result = cli.invoke(app, [
        "sync", "default", "--",
        "sh", "-c", "echo synced",
    ])
    assert result.exit_code == 0, result.output

    # push should have copied catalog and fdl.toml to target
    assert (storage / "test_ds" / "ducklake.duckdb").exists()
    assert (storage / "test_ds" / "fdl.toml").exists()

    # meta.json should exist (push writes it)
    remote_meta = storage / "test_ds" / FDL_DIR / META_JSON
    assert remote_meta.exists()
    data = json.loads(remote_meta.read_text())
    assert "pushed_at" in data


def test_sync_with_explicit_command(fdl_project_dir: Path):
    """fdl sync TARGET -- COMMAND uses the explicit command."""
    cli = CliRunner()
    _init_project(fdl_project_dir, cli)

    marker = fdl_project_dir / "marker.txt"
    result = cli.invoke(app, [
        "sync", "default", "--",
        "sh", "-c", f"echo hello > {marker}",
    ])
    assert result.exit_code == 0, result.output
    assert marker.read_text().strip() == "hello"


def test_sync_reads_command_from_toml(fdl_project_dir: Path):
    """fdl sync TARGET (no --) reads command from fdl.toml."""
    cli = CliRunner()
    storage = _init_project(fdl_project_dir, cli)

    marker = fdl_project_dir / "pipeline_ran.txt"
    set_value("command", f"sh -c 'echo done > {marker}'")

    result = cli.invoke(app, ["sync", "default"])
    assert result.exit_code == 0, result.output
    assert marker.exists()
    assert marker.read_text().strip() == "done"

    # push should have happened
    assert (storage / "test_ds" / "ducklake.duckdb").exists()


def test_sync_skips_push_on_run_failure(fdl_project_dir: Path):
    """fdl sync does NOT push when the command fails (non-zero exit)."""
    cli = CliRunner()
    storage = _init_project(fdl_project_dir, cli)

    result = cli.invoke(app, [
        "sync", "default", "--",
        "python", "-c", "raise SystemExit(1)",
    ])
    assert result.exit_code == 1

    # push should NOT have happened
    assert not (storage / "test_ds" / "ducklake.duckdb").exists()


def test_sync_no_command_configured_and_no_explicit_command_fails(fdl_project_dir: Path):
    """fdl sync TARGET without command in fdl.toml and without -- COMMAND fails."""
    cli = CliRunner()
    _init_project(fdl_project_dir, cli)

    result = cli.invoke(app, ["sync", "default"])
    assert result.exit_code != 0
    assert "command" in result.output.lower()
