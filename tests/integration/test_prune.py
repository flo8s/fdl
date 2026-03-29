"""Integration tests for fdl prune.

Spec (docs/reference/cli.md#prune):
  fdl prune TARGET [--dry-run] [--force] [--older-than DAYS]
  - S3 targets only (rejects local targets)
  - Deletes orphaned files not referenced by any active snapshot
  - --dry-run lists orphans without deleting

Uses moto for in-memory S3.
"""

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from fdl import DUCKLAKE_FILE, FDL_DIR, ducklake_data_path
from fdl.cli import app
from fdl.prune import _get_active_files, prune_datasource
from fdl.s3 import S3Config

BUCKET = "test-bucket"
DATASOURCE = "test_ds"
S3 = S3Config(
    bucket=BUCKET,
    endpoint="https://s3.us-east-1.amazonaws.com",
    access_key_id="testing",
    secret_access_key="testing",
)


def _data_prefix() -> str:
    return f"{DATASOURCE}/{ducklake_data_path(DUCKLAKE_FILE)}"


def _list_s3_keys(client, prefix: str = "") -> list[str]:
    resp = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]


@pytest.fixture
def moto_s3(monkeypatch):
    """In-memory S3 via moto with a pre-created bucket.

    Patches create_s3_client because moto doesn't support custom endpoint_url.
    """
    def _factory(s3):
        return boto3.client(
            "s3", region_name="us-east-1",
            aws_access_key_id=s3.access_key_id,
            aws_secret_access_key=s3.secret_access_key,
        )

    with mock_aws():
        monkeypatch.setattr("fdl.s3.create_s3_client", _factory)
        client = _factory(S3)
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def s3_project(fdl_project_dir):
    """Init a project and insert data so the catalog has active files."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", DATASOURCE,
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (1), (2), (3)"])
    return fdl_project_dir


# --- CLI error cases ---


def test_local_target_is_rejected(fdl_project_dir):
    """prune rejects non-S3 targets."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", DATASOURCE,
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])
    result = cli.invoke(app, ["prune", "default"])
    assert result.exit_code != 0
    assert "S3" in result.output


def test_without_init_fails(fdl_project_dir):
    """prune fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["prune", "default"])
    assert result.exit_code != 0


# --- Pruning logic (moto S3) ---


def test_deletes_orphaned_files(s3_project, moto_s3):
    """Orphaned files are deleted; active files survive."""
    active = _get_active_files(s3_project / FDL_DIR / DUCKLAKE_FILE)
    assert len(active) > 0, "Catalog should have active files after INSERT"

    prefix = _data_prefix()
    for f in active:
        moto_s3.put_object(Bucket=BUCKET, Key=f"{prefix}{f}", Body=b"active")

    orphan_key = f"{prefix}main/t/orphan_00000.parquet"
    moto_s3.put_object(Bucket=BUCKET, Key=orphan_key, Body=b"orphan")

    prune_datasource(s3_project, s3_project / FDL_DIR, s3=S3, force=True)

    remaining = _list_s3_keys(moto_s3, prefix)
    assert orphan_key not in remaining
    for f in active:
        assert f"{prefix}{f}" in remaining


def test_dry_run_does_not_delete(s3_project, moto_s3):
    """--dry-run lists orphans but does not delete them."""
    prefix = _data_prefix()
    orphan_key = f"{prefix}main/t/orphan.parquet"
    moto_s3.put_object(Bucket=BUCKET, Key=orphan_key, Body=b"orphan")

    prune_datasource(s3_project, s3_project / FDL_DIR, s3=S3, dry_run=True)

    assert orphan_key in _list_s3_keys(moto_s3, prefix)


def test_no_orphans_leaves_files_unchanged(s3_project, moto_s3):
    """When all S3 files are active, prune changes nothing."""
    active = _get_active_files(s3_project / FDL_DIR / DUCKLAKE_FILE)
    prefix = _data_prefix()
    for f in active:
        moto_s3.put_object(Bucket=BUCKET, Key=f"{prefix}{f}", Body=b"data")

    before = _list_s3_keys(moto_s3, prefix)
    prune_datasource(s3_project, s3_project / FDL_DIR, s3=S3, force=True)
    assert _list_s3_keys(moto_s3, prefix) == before


def test_missing_catalog_raises(fdl_project_dir, moto_s3):
    """prune_datasource fails when .fdl/ducklake.duckdb is missing."""
    from fdl.config import set_value

    set_value("name", DATASOURCE, fdl_project_dir / "fdl.toml")
    (fdl_project_dir / FDL_DIR).mkdir()

    with pytest.raises(FileNotFoundError, match="fdl pull"):
        prune_datasource(fdl_project_dir, fdl_project_dir / FDL_DIR, s3=S3, force=True)
