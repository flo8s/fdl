"""Shared fixtures for integration tests."""

from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

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
