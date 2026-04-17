"""Integration tests for S3 targets (push, pull, checkpoint).

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
    """fdl push uploads ducklake.duckdb to S3."""
    result = CliRunner().invoke(app, ["push", "default"])
    assert result.exit_code == 0, result.output

    keys = [
        obj["Key"] for obj in moto_s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    ]
    assert "test_ds/ducklake.duckdb" in keys
    assert "test_ds/fdl.toml" in keys


def test_push_conflict_on_s3(s3_project, moto_s3):
    """Push conflict detection works with S3 targets."""
    cli = CliRunner()
    cli.invoke(app, ["push", "default"])

    # Simulate another user's push
    moto_s3.put_object(
        Bucket=BUCKET,
        Key=f"test_ds/{FDL_DIR}/{META_JSON}",
        Body=json.dumps({"pushed_at": "2099-01-01T00:00:00+00:00"}).encode(),
    )

    result = cli.invoke(app, ["push", "default"])
    assert result.exit_code != 0


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


# NOTE: S3 checkpoint is not tested here because DuckDB's CHECKPOINT
# uses httpfs internally, which cannot reach moto's in-process mock.
# S3 checkpoint is a DuckDB/DuckLake concern, not fdl logic.
