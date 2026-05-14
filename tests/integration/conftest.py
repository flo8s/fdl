"""Shared fixtures for integration tests."""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

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
    """In-memory S3 via moto with a pre-created bucket."""
    with mock_aws():
        monkeypatch.setattr("fdl.s3.create_s3_client", _moto_client_factory)
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client
