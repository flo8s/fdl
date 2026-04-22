"""Integration tests for fdl run.

Spec (docs/reference/cli.md#run):
  fdl run TARGET -- COMMAND [ARGS...]
  - Injects FDL_CATALOG_URL / FDL_CATALOG_PATH / FDL_DATA_URL (always)
    and FDL_DATA_BUCKET / FDL_DATA_PREFIX / FDL_S3_* (S3 targets) into subprocess
  - Existing environment variables are NOT overwritten
"""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_fdl_catalog_path_points_to_local_file(fdl_project_dir: Path):
    """fdl run injects FDL_CATALOG_PATH as an absolute path to the SQLite file."""
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
        "sh", "-c", f"echo $FDL_CATALOG_PATH > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    catalog = env_file.read_text().strip()
    assert catalog.endswith("ducklake.sqlite")
    assert Path(catalog).is_absolute()
    assert Path(catalog).exists()


def test_fdl_catalog_url_is_sqlite_scheme(fdl_project_dir: Path):
    """fdl run injects FDL_CATALOG_URL as a sqlite:///<abs> URL."""
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
        "sh", "-c", f"echo $FDL_CATALOG_URL > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    url = env_file.read_text().strip()
    assert url.startswith("sqlite:///")
    assert url.endswith("/ducklake.sqlite")


def test_existing_env_vars_are_not_overwritten(fdl_project_dir: Path, monkeypatch):
    """Pre-set FDL_DATA_URL is preserved, enabling CI/CD pre-configuration."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    monkeypatch.setenv("FDL_DATA_URL", "custom_value")
    env_file = fdl_project_dir / "env_out.txt"
    result = cli.invoke(app, [
        "run", "default", "--",
        "sh", "-c", f"echo $FDL_DATA_URL > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    assert env_file.read_text().strip() == "custom_value"


def test_fdl_data_url_ends_with_files(fdl_project_dir: Path):
    """fdl run injects FDL_DATA_URL derived from target storage.

    Spec: FDL_DATA_URL = {target_storage}/ducklake.duckdb.files/
    """
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
        "sh", "-c", f"echo $FDL_DATA_URL > {env_file}",
    ])
    assert result.exit_code == 0, result.output
    data_url = env_file.read_text().strip()
    assert data_url.endswith("ducklake.duckdb.files/")
    assert str(storage) in data_url


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


def test_run_errors_when_target_has_no_catalog(fdl_project_dir: Path):
    """fdl run fails with a helpful message when neither init nor pull has produced a catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    # Add a second target without a catalog in the (empty) remote.
    from fdl.config import set_value
    set_value("targets.local.url", str(storage), fdl_project_dir / "fdl.toml")
    set_value("targets.local.public_url", "http://localhost:4001", fdl_project_dir / "fdl.toml")

    result = cli.invoke(app, ["run", "local", "--", "sh", "-c", "true"])
    assert result.exit_code != 0
    assert "fdl init" in result.output
    assert "fdl pull" in result.output
    # Regression guard: pull_if_needed used to return "No local catalog" even
    # when the pull did not actually materialize one, causing a misleading
    # "pulled from <target>" log line right above the error.
    assert "pulled from" not in result.output


def test_run_reads_command_from_toml(fdl_project_dir: Path):
    """fdl run TARGET (no --) reads command from fdl.toml."""
    from fdl.config import set_value

    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    marker = fdl_project_dir / "ran.txt"
    set_value("command", f"sh -c 'echo ok > {marker}'")

    result = cli.invoke(app, ["run", "default"])
    assert result.exit_code == 0, result.output
    assert marker.read_text().strip() == "ok"
