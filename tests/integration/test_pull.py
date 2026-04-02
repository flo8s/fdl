"""Integration tests for fdl pull.

Spec (docs/reference/cli.md#pull):
  fdl pull TARGET
  - Downloads catalog from target
  - Requires prior fdl init
  - Syncs .fdl/meta.json from remote (enables conflict detection on next push)
"""

import json

from typer.testing import CliRunner

from fdl import FDL_DIR, META_JSON
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
    (fdl_project_dir / ".fdl" / "default" / "ducklake.duckdb").unlink()

    # Pull and verify data survived
    cli.invoke(app, ["pull", "default"])
    result = cli.invoke(app, ["sql", "default", "SELECT x FROM t"])
    assert result.exit_code == 0
    assert "42" in result.output


def test_pull_syncs_meta_json(fdl_project_dir):
    """After pull, local meta.json matches the remote's pushed_at timestamp.

    This enables conflict detection: if someone else pushes between your
    pull and push, the timestamps won't match and push will be rejected.
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
    remote_meta = storage / "test_ds" / FDL_DIR / META_JSON
    remote_pushed_at = json.loads(remote_meta.read_text())["pushed_at"]

    # Delete local meta.json to simulate a fresh clone
    local_meta = fdl_project_dir / FDL_DIR / "default" / META_JSON
    local_meta.unlink()

    cli.invoke(app, ["pull", "default"])
    local_pushed_at = json.loads(local_meta.read_text())["pushed_at"]
    assert local_pushed_at == remote_pushed_at


def test_pull_then_push_succeeds(fdl_project_dir):
    """The pull → push workflow succeeds (meta.json is properly synced)."""
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
    """fdl pull from a target that was never pushed to does not create a catalog.

    NOTE: Currently pull silently succeeds (exit 0) even when the source has
    no catalog. pull_from_local returns False but the CLI ignores it.
    """
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    # Delete local catalog without pushing first
    (fdl_project_dir / ".fdl" / "default" / "ducklake.duckdb").unlink()

    # Pull from empty target
    cli.invoke(app, ["pull", "default"])
    assert not (fdl_project_dir / ".fdl" / "default" / "ducklake.duckdb").exists()
