"""Integration tests for fdl init."""

from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def test_default_catalog_is_duckdb(fdl_project_dir: Path):
    """fdl init creates a DuckDB catalog by default."""
    import duckdb

    result = CliRunner().invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    assert result.exit_code == 0, result.output
    assert (fdl_project_dir / "fdl.toml").exists()
    assert (fdl_project_dir / ".fdl").is_dir()

    db_path = fdl_project_dir / ".fdl" / "ducklake.duckdb"
    assert db_path.exists()
    conn = duckdb.connect(str(db_path), read_only=True)
    tables = conn.execute("SHOW TABLES").fetchall()
    conn.close()
    assert len(tables) > 0


def test_invalid_name_is_rejected_with_suggestion(fdl_project_dir: Path):
    """fdl init rejects invalid SQL identifier and suggests sanitized name."""
    result = CliRunner().invoke(app, [
        "init", "my-data",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    assert result.exit_code != 0
    assert "my_data" in result.output


def test_existing_toml_prevents_init(fdl_project_dir: Path):
    """fdl init fails if fdl.toml already exists."""
    (fdl_project_dir / "fdl.toml").write_text('name = "existing"\n')
    result = CliRunner().invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    assert result.exit_code != 0
    assert "already exists" in result.output


def test_double_init_preserves_existing_files(fdl_project_dir: Path):
    """Double init fails without deleting the existing project files."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    original_toml = (fdl_project_dir / "fdl.toml").read_text()

    result = cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    assert result.exit_code != 0
    assert (fdl_project_dir / "fdl.toml").read_text() == original_toml
    assert (fdl_project_dir / ".fdl" / "ducklake.duckdb").exists()


def test_failure_cleans_up_partial_files(fdl_project_dir: Path):
    """fdl init cleans up fdl.toml and .fdl/ on failure.

    A single quote in public_url breaks the DuckDB SQL in init_ducklake,
    triggering the rollback path after fdl.toml has already been created.
    """
    result = CliRunner().invoke(app, [
        "init", "test_ds",
        "--public-url", "http://it's.broken",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    assert result.exit_code != 0
    assert not (fdl_project_dir / "fdl.toml").exists()
    assert not (fdl_project_dir / ".fdl").exists()


def test_sqlite_flag_creates_sqlite_catalog(fdl_project_dir: Path):
    """fdl init --sqlite creates a SQLite catalog."""
    import sqlite3

    result = CliRunner().invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
        "--sqlite",
    ])
    assert result.exit_code == 0, result.output

    db_path = fdl_project_dir / ".fdl" / "ducklake.sqlite"
    assert db_path.exists()
    conn = sqlite3.connect(db_path)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()
    assert len(tables) > 0
