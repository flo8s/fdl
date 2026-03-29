"""Integration tests for fdl run.

Spec (docs/reference/cli.md#run):
  fdl run TARGET -- COMMAND [ARGS...]
  - Injects FDL_STORAGE, FDL_DATA_PATH, FDL_CATALOG, FDL_S3_* into subprocess
  - Existing environment variables are NOT overwritten
"""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_fdl_storage_is_injected(fdl_project_dir: Path):
    """fdl run injects FDL_STORAGE into the subprocess environment."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    env_file = fdl_project_dir / "env_out.txt"
    result = cli.invoke(app, [
        "run", "default", "--",
        "sh", "-c", f"echo $FDL_STORAGE > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    content = env_file.read_text().strip()
    assert str(storage) in content
    assert "test_ds" in content


def test_existing_env_vars_are_not_overwritten(fdl_project_dir: Path, monkeypatch):
    """Pre-set FDL_STORAGE is preserved, enabling CI/CD pre-configuration."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    monkeypatch.setenv("FDL_STORAGE", "custom_value")
    env_file = fdl_project_dir / "env_out.txt"
    result = cli.invoke(app, [
        "run", "default", "--",
        "sh", "-c", f"echo $FDL_STORAGE > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    assert env_file.read_text().strip() == "custom_value"


def test_missing_separator_fails(fdl_project_dir: Path):
    """fdl run without -- separator fails."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["run", "default", "echo", "hello"])
    assert result.exit_code != 0


def test_empty_command_after_separator_fails(fdl_project_dir: Path):
    """fdl run with empty command after -- fails."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["run", "default", "--"])
    assert result.exit_code != 0


def test_subprocess_exit_code_is_propagated(fdl_project_dir: Path):
    """fdl run propagates the subprocess exit code."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, [
        "run", "default", "--",
        "python", "-c", "raise SystemExit(42)",
    ])
    assert result.exit_code == 42
