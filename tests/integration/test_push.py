"""Integration tests for fdl push (local targets).

Spec (docs/reference/cli.md#push):
  fdl push TARGET [--force]
  - Pushes ducklake.duckdb and fdl.toml to the target
  - Data files are NOT included
  - The local SQLite catalog is converted to DuckDB during push; the remote
    always receives DuckDB format
  - Conflict detection applies to S3 targets only (see test_s3.py);
    local targets are assumed single-user
"""

from pathlib import Path

from typer.testing import CliRunner

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


def test_push_uploads_only_duckdb_format(fdl_project_dir: Path):
    """Push converts the local SQLite catalog to DuckDB; remote has only DuckDB."""
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
    assert not (storage / "test_ds" / "ducklake.sqlite").exists()


def test_second_push_after_first_succeeds(fdl_project_dir: Path):
    """Consecutive pushes from the same user succeed."""
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


def _read_data_path(catalog: Path) -> str:
    import duckdb

    conn = duckdb.connect(str(catalog))
    try:
        rows = conn.execute(
            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    return rows[0][0]


def test_push_rewrites_data_path_to_match_public_url(fdl_project_dir: Path):
    """Push updates ducklake_metadata.data_path to the URL derived from public_url."""
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

    catalog = storage / "test_ds" / "ducklake.duckdb"
    assert _read_data_path(catalog) == \
        "http://localhost:4001/test_ds/ducklake.duckdb.files/"


def test_push_updates_data_path_after_public_url_change(fdl_project_dir: Path):
    """Updating public_url and pushing again rewrites data_path in the shipped catalog."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["push", "default"])

    cli.invoke(app, ["config", "targets.default.public_url", "https://data.example.com"])
    cli.invoke(app, ["push", "default"])

    catalog = storage / "test_ds" / "ducklake.duckdb"
    assert _read_data_path(catalog) == \
        "https://data.example.com/test_ds/ducklake.duckdb.files/"


def test_push_does_not_touch_local_sqlite_data_path(fdl_project_dir: Path):
    """Local SQLite catalog's data_path is untouched by push (ships via a copy)."""
    import sqlite3

    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])

    local = fdl_project_dir / ".fdl" / "default" / "ducklake.sqlite"
    conn = sqlite3.connect(local)
    row = conn.execute(
        "SELECT value FROM ducklake_metadata WHERE key = 'data_path'"
    ).fetchone()
    conn.close()
    before = row[0]

    cli.invoke(app, ["push", "default"])

    conn = sqlite3.connect(local)
    row = conn.execute(
        "SELECT value FROM ducklake_metadata WHERE key = 'data_path'"
    ).fetchone()
    conn.close()
    assert row[0] == before


def test_push_does_not_alter_other_metadata_keys(fdl_project_dir: Path):
    """Only the data_path row is changed; version/encrypted/etc. are preserved."""
    import duckdb

    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["push", "default"])

    catalog = storage / "test_ds" / "ducklake.duckdb"
    conn = duckdb.connect(str(catalog))
    try:
        rows_before = {
            k: v
            for k, v in conn.execute(
                "SELECT key, value FROM ducklake_metadata WHERE key != 'data_path'"
            ).fetchall()
        }
    finally:
        conn.close()

    cli.invoke(app, ["config", "targets.default.public_url", "https://new.example.com"])
    cli.invoke(app, ["push", "default"])

    conn = duckdb.connect(str(catalog))
    try:
        rows_after = {
            k: v
            for k, v in conn.execute(
                "SELECT key, value FROM ducklake_metadata WHERE key != 'data_path'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert rows_before == rows_after
    assert rows_before  # not empty
