"""Integration tests for S3 targets (push, pull, conflict detection).

Uses moto for in-memory S3 via the `moto_s3` / `s3_project` fixtures
from tests/integration/conftest.py.
"""

import json

from typer.testing import CliRunner

from fdl import FDL_DIR, META_JSON
from fdl.cli import app

BUCKET = "test-bucket"


# --- push ---


def test_push_uploads_catalog_to_s3(s3_project, moto_s3):
    """fdl push uploads ducklake.duckdb and fdl.toml to S3."""
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    keys = {
        obj["Key"] for obj in moto_s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    }
    assert "test_ds/ducklake.duckdb" in keys
    assert "test_ds/fdl.toml" in keys


def test_push_saves_etag_to_local_state(s3_project, moto_s3):
    """After a successful push, the local state file holds the remote ETag."""
    CliRunner().invoke(app, ["push", "default"])

    state_path = s3_project / FDL_DIR / "default" / META_JSON
    assert state_path.exists()
    saved = json.loads(state_path.read_text())
    assert "remote_etag" in saved

    head = moto_s3.head_object(Bucket=BUCKET, Key="test_ds/ducklake.duckdb")
    assert saved["remote_etag"] == head["ETag"]


def test_push_conflict_on_s3(s3_project, moto_s3):
    """Push is rejected when another client has replaced the catalog (ETag mismatch)."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Simulate another user's push by overwriting the catalog object; this
    # changes the server-side ETag.
    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"tampered catalog bytes",
    )

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code != 0
    assert "pull" in result.output.lower()


def test_force_push_overrides_s3_conflict(s3_project, moto_s3):
    """--force bypasses the If-Match precondition."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"tampered catalog bytes",
    )

    result = cli.invoke(app, ["push", "--force", "default"])
    assert result.exit_code == 0, result.output


def test_first_push_rejected_when_remote_already_exists(s3_project, moto_s3):
    """Initial push (no local ETag) uses IfNoneMatch="*" and fails if remote exists."""
    # Simulate a prior push from somewhere else — local has no ETag recorded.
    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"prior upload from another client",
    )

    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code != 0
    assert "pull" in result.output.lower()


# --- pull ---


def test_pull_restores_catalog_from_s3(s3_project, moto_s3):
    """fdl pull downloads DuckDB from S3 and converts it to SQLite locally."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Drop everything local so pull_if_needed actually hits the network.
    (s3_project / ".fdl" / "default" / "ducklake.sqlite").unlink()
    (s3_project / ".fdl" / "default" / "ducklake.duckdb").unlink(missing_ok=True)

    result = cli.invoke(app, ["pull", "default"])
    assert result.exit_code == 0, result.output
    assert (s3_project / ".fdl" / "default" / "ducklake.sqlite").exists()
    assert not (s3_project / ".fdl" / "default" / "ducklake.duckdb").exists()


def test_pull_saves_etag_to_local_state(s3_project, moto_s3):
    """After pull, local state matches the server ETag of the catalog."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Remove local state to simulate a fresh clone; then pull should restore it.
    state_path = s3_project / FDL_DIR / "default" / META_JSON
    state_path.unlink()

    cli.invoke(app, ["pull", "default"])

    saved = json.loads(state_path.read_text())
    head = moto_s3.head_object(Bucket=BUCKET, Key="test_ds/ducklake.duckdb")
    assert saved["remote_etag"] == head["ETag"]


def test_pull_preserves_etag_when_conversion_fails(s3_project, moto_s3, monkeypatch):
    """Failed DuckDB->SQLite conversion must not update the stored ETag.

    Regression guard: fetch_from_s3 used to record the new ETag before
    running the conversion. A crash mid-conversion left the local
    ducklake.sqlite gone and meta.json bumped to the new remote ETag, so
    the next fdl pull reported "Already up to date" and the project was
    stuck on a DuckDB-shaped local catalog (violating the SQLite-only
    contract dlt relies on).
    """
    import duckdb

    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Change the remote so its ETag diverges from the one we just stored.
    local_duckdb = s3_project / ".fdl" / "default" / "ducklake.duckdb"
    conn = duckdb.connect(str(local_duckdb))
    conn.execute(
        "UPDATE ducklake_metadata SET value = 'external' WHERE key = 'created_by'"
    )
    conn.close()
    moto_s3.upload_file(str(local_duckdb), BUCKET, "test_ds/ducklake.duckdb")

    state_path = s3_project / FDL_DIR / "default" / META_JSON
    etag_before = json.loads(state_path.read_text())["remote_etag"]

    # Break the conversion step.
    def _boom(*args, **kwargs):
        raise RuntimeError("simulated conversion failure")

    monkeypatch.setattr("fdl.ducklake.convert_duckdb_to_sqlite", _boom)

    result = cli.invoke(app, ["pull", "default"])
    assert result.exit_code != 0

    # ETag must still point at the pre-pull value, so the next pull retries.
    etag_after = json.loads(state_path.read_text())["remote_etag"]
    assert etag_after == etag_before


def test_pull_then_push_succeeds_after_external_change(s3_project, moto_s3):
    """After a conflict, pull resyncs state so the next push succeeds.

    Uses a small in-place mutation of the DuckDB catalog to produce a valid
    catalog with a different ETag, simulating another client's push.
    """
    import duckdb

    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    local_duckdb = s3_project / ".fdl" / "default" / "ducklake.duckdb"
    conn = duckdb.connect(str(local_duckdb))
    conn.execute(
        "UPDATE ducklake_metadata SET value = 'external' WHERE key = 'created_by'"
    )
    conn.close()
    moto_s3.upload_file(str(local_duckdb), BUCKET, "test_ds/ducklake.duckdb")

    assert cli.invoke(app, ["push", "default"]).exit_code != 0

    cli.invoke(app, ["pull", "default"])
    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output


# --- sql (stale-catalog detection) ---


def test_sql_rejects_stale_catalog(s3_project, moto_s3):
    """fdl sql refuses to run when the remote catalog ETag has diverged."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Simulate another client overwriting the catalog.
    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"external change",
    )

    result = cli.invoke(app, ["sql", "default", "SELECT 1"])
    assert result.exit_code != 0
    assert "fdl pull" in result.output.lower()


def test_sql_force_skips_freshness_check(s3_project, moto_s3):
    """--force lets sql run even when the remote catalog has diverged."""
    cli = CliRunner()
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["push", "default"])

    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"external change",
    )

    result = cli.invoke(app, ["sql", "default", "--force", "SELECT 1"])
    assert result.exit_code == 0, result.output


# NOTE: S3 checkpoint is not tested here because DuckDB's CHECKPOINT
# uses httpfs internally, which cannot reach moto's in-process mock.
# S3 checkpoint is a DuckDB/DuckLake concern, not fdl logic.
