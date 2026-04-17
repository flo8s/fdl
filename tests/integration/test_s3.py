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
    """fdl pull downloads catalog from S3."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Delete local catalog
    (s3_project / ".fdl" / "default" / "ducklake.duckdb").unlink()

    result = cli.invoke(app, ["pull", "default"])
    assert result.exit_code == 0, result.output
    assert (s3_project / ".fdl" / "default" / "ducklake.duckdb").exists()


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


def test_pull_then_push_succeeds_after_external_change(s3_project, moto_s3):
    """After a conflict, pull resyncs state so the next push succeeds."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # External change invalidates our saved ETag.
    moto_s3.put_object(
        Bucket=BUCKET,
        Key="test_ds/ducklake.duckdb",
        Body=b"external change",
    )

    # Without pull, push is rejected.
    assert cli.invoke(app, ["push", "default"]).exit_code != 0

    # Pull resyncs the ETag; subsequent push is accepted (same content, but
    # the precondition now matches the current remote ETag).
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
