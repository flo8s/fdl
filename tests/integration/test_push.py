"""Integration tests for fdl push.

Spec (docs/reference/cli.md#push):
  fdl push TARGET [--force]
  - Pushes ducklake.duckdb and fdl.toml to the target
  - Data files are NOT included
  - SQLite catalogs are automatically converted to DuckDB during push
  - Conflict detection: rejects push if remote was updated since last pull
  - --force overrides conflict detection
  - First push to a target with no meta.json always succeeds
"""

import json
from pathlib import Path

from typer.testing import CliRunner

from fdl import FDL_DIR, META_JSON
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


def test_first_push_creates_meta_json(fdl_project_dir: Path):
    """First push writes meta.json to both remote and local .fdl/."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    cli.invoke(app, ["push", "default"])

    # Remote meta.json
    remote_meta = storage / "test_ds" / FDL_DIR / META_JSON
    assert remote_meta.exists()
    remote_data = json.loads(remote_meta.read_text())
    assert "pushed_at" in remote_data

    # Local meta.json
    local_meta = fdl_project_dir / FDL_DIR / META_JSON
    assert local_meta.exists()
    local_data = json.loads(local_meta.read_text())
    assert local_data["pushed_at"] == remote_data["pushed_at"]


def test_second_push_after_first_succeeds(fdl_project_dir: Path):
    """Consecutive pushes from the same user succeed (timestamps match)."""
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


def test_push_conflict_is_rejected(fdl_project_dir: Path):
    """Push is rejected when remote was updated by someone else.

    Simulates another user's push by modifying the remote meta.json
    with a different pushed_at timestamp.
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

    # Simulate another user's push by changing the remote timestamp
    remote_meta = storage / "test_ds" / FDL_DIR / META_JSON
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code != 0
    assert "fdl pull" in result.output


def test_force_push_overrides_conflict(fdl_project_dir: Path):
    """--force allows push even when remote has diverged."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    cli.invoke(app, ["push", "default"])

    # Simulate another user's push
    remote_meta = storage / "test_ds" / FDL_DIR / META_JSON
    remote_meta.write_text(json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}))

    result = cli.invoke(app, ["push", "--force", "default"])
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
