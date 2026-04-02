"""Integration tests for S3 targets (push, pull, checkpoint).

Uses moto for in-memory S3. Patches create_s3_client because moto
doesn't support custom endpoint_url used by fdl's S3 config.
"""

import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from fdl import FDL_DIR, META_JSON
from fdl.cli import app

BUCKET = "test-bucket"


def _moto_client_factory(s3):
    """Create a moto-compatible S3 client (no custom endpoint)."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=s3.access_key_id,
        aws_secret_access_key=s3.secret_access_key,
    )


@pytest.fixture
def moto_s3(monkeypatch):
    """In-memory S3 via moto with a pre-created bucket.

    Patches create_s3_client because moto doesn't support custom endpoint_url.
    """
    with mock_aws():
        monkeypatch.setattr("fdl.s3.create_s3_client", _moto_client_factory)
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def s3_project(fdl_project_dir: Path, moto_s3):
    """Init a project with an S3 target."""
    cli = CliRunner()
    cli.invoke(
        app,
        [
            "init",
            "test_ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            f"s3://{BUCKET}",
            "--target-name",
            "default",
        ],
    )
    cli.invoke(
        app,
        [
            "config",
            "targets.default.s3_endpoint",
            "https://s3.us-east-1.amazonaws.com",
        ],
    )
    cli.invoke(
        app,
        [
            "config",
            "targets.default.s3_access_key_id",
            "testing",
        ],
    )
    cli.invoke(
        app,
        [
            "config",
            "targets.default.s3_secret_access_key",
            "testing",
        ],
    )
    return fdl_project_dir


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
