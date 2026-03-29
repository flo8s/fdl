"""Integration tests for fdl config.

Spec (docs/reference/cli.md#config):
  fdl config [KEY] [VALUE]
  - KEY VALUE: set a value in fdl.toml
  - KEY only: display current value
  - no args: list all settings
  - ${VAR} references are stored literally (expanded at runtime by resolve_target etc.)
"""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_get_returns_value_created_by_init(fdl_project_dir: Path):
    """fdl config KEY returns a value that fdl init wrote to fdl.toml."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["config", "name"])
    assert result.exit_code == 0
    assert "test_ds" in result.output


def test_set_value_is_retrievable(fdl_project_dir: Path):
    """fdl config KEY VALUE then fdl config KEY returns the set value."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    cli.invoke(app, ["config", "targets.default.url", "/new/path"])
    result = cli.invoke(app, ["config", "targets.default.url"])
    assert result.exit_code == 0
    assert "/new/path" in result.output


def test_no_args_lists_all_settings_with_correct_values(fdl_project_dir: Path):
    """fdl config (no args) lists all settings with the values set by init."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["config"])
    assert result.exit_code == 0
    assert "name=test_ds" in result.output
    assert "catalog=duckdb" in result.output
    assert "targets.default.public_url=http://localhost:4001" in result.output


def test_env_var_reference_is_stored_literally(fdl_project_dir: Path):
    """${VAR} in config values is stored as-is, not expanded at write time.

    This is how S3 credentials are managed: the literal ${VAR} is saved in
    fdl.toml (which is git-tracked), and expanded at runtime.
    """
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    cli.invoke(app, ["config", "targets.default.s3_endpoint", "${FDL_S3_ENDPOINT}"])
    result = cli.invoke(app, ["config", "targets.default.s3_endpoint"])
    assert result.exit_code == 0
    assert "${FDL_S3_ENDPOINT}" in result.output


def test_s3_credentials_roundtrip(fdl_project_dir: Path):
    """S3 credentials can be configured and retrieved via fdl config."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    cli.invoke(app, ["config", "targets.default.s3_endpoint", "${FDL_S3_ENDPOINT}"])
    cli.invoke(app, ["config", "targets.default.s3_access_key_id", "${FDL_S3_ACCESS_KEY_ID}"])
    cli.invoke(app, ["config", "targets.default.s3_secret_access_key", "${FDL_S3_SECRET_ACCESS_KEY}"])

    result = cli.invoke(app, ["config"])
    assert "targets.default.s3_endpoint=${FDL_S3_ENDPOINT}" in result.output
    assert "targets.default.s3_access_key_id=${FDL_S3_ACCESS_KEY_ID}" in result.output
    assert "targets.default.s3_secret_access_key=${FDL_S3_SECRET_ACCESS_KEY}" in result.output


def test_missing_key_exits_with_error(fdl_project_dir: Path):
    """fdl config with a nonexistent key exits with code 1."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["config", "nonexistent.key"])
    assert result.exit_code == 1


def test_without_init_fails(fdl_project_dir: Path):
    """fdl config fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["config"])
    assert result.exit_code != 0
