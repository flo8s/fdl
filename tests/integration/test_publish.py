"""Integration tests for fdl publish (local + S3 moto)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import duckdb
import pytest

import fdl
from fdl import DUCKLAKE_FILE, FDL_DIR, META_JSON
from fdl.config import CatalogSpec
from fdl.ducklake import build_attach_sql, init_ducklake
from fdl.meta import PushConflictError

from tests.integration.conftest import BUCKET


def _setup_live_project(project_dir: Path, *, publish_url: str) -> None:
    """Write a v0.11 fdl.toml and create a sqlite live catalog with one row."""
    sqlite_path = project_dir / FDL_DIR / "ducklake.sqlite"
    data_dir = project_dir / FDL_DIR / "data"
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    (project_dir / "fdl.toml").write_text(
        f'name = "ds"\n\n'
        f"[metadata]\n"
        f'url = "sqlite:///{sqlite_path.resolve()}"\n\n'
        f"[data]\n"
        f'url = "{data_dir.resolve()}"\n\n'
        f"[publishes.default]\n"
        f'url = "{publish_url}"\n'
    )

    spec = CatalogSpec(
        scheme="sqlite",
        raw=f"sqlite:///{sqlite_path}",
        path=str(sqlite_path),
    )
    init_ducklake(spec, str(data_dir), "ds")

    conn = duckdb.connect()
    try:
        for s in build_attach_sql(
            metadata=spec, data_url=str(data_dir), datasource="ds",
        ):
            conn.execute(s)
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (7)")
    finally:
        conn.close()


class TestPublishLocal:
    def test_local_publish_round_trip(self, fdl_project_dir, tmp_path):
        dest = tmp_path / "pub"
        _setup_live_project(fdl_project_dir, publish_url=str(dest))

        fdl.publish(project_dir=fdl_project_dir)

        assert (dest / "fdl.toml").exists()
        assert (dest / DUCKLAKE_FILE).exists()
        # fdl.toml content was copied verbatim
        data = tomllib.loads((dest / "fdl.toml").read_text())
        assert data["name"] == "ds"

        # A downstream project pulls from the publish dir to rebuild its
        # local SQLite live catalog.
        mirror = tmp_path / "mirror"
        mirror.mkdir()
        mirror_sqlite = mirror / FDL_DIR / "ducklake.sqlite"
        src_data = (fdl_project_dir / FDL_DIR / "data").resolve()
        (mirror / "fdl.toml").write_text(
            f'name = "ds"\n\n'
            f"[metadata]\n"
            f'url = "sqlite:///{mirror_sqlite.resolve()}"\n\n'
            f"[data]\n"
            f'url = "{src_data}"\n\n'
            f"[publishes.default]\n"
            f'url = "{dest}"\n'
        )

        fdl.pull(project_dir=mirror)

        assert mirror_sqlite.exists()
        with fdl.connect(project_dir=mirror) as conn:
            rows = conn.execute("SELECT x FROM t").fetchall()
        assert rows == [(7,)]

    def test_intermediate_cleanup(self, fdl_project_dir, tmp_path):
        dest = tmp_path / "pub"
        _setup_live_project(fdl_project_dir, publish_url=str(dest))

        fdl.publish(project_dir=fdl_project_dir)
        # Intermediate duckdb is cleaned up by default
        intermediate = fdl_project_dir / FDL_DIR / "publishes" / "default" / DUCKLAKE_FILE
        assert not intermediate.exists()

    def test_empty_publishes_errors(self, fdl_project_dir):
        (fdl_project_dir / "fdl.toml").write_text(
            'name = "ds"\n[metadata]\nurl = "sqlite:///x"\n[data]\nurl = "y"\n'
        )
        with pytest.raises(KeyError):
            fdl.publish(project_dir=fdl_project_dir)


class TestPublishS3:
    def test_s3_publish_writes_objects_and_etag(
        self, fdl_project_dir, tmp_path, moto_s3
    ):
        publish_url = f"s3://{BUCKET}/myds"
        _setup_live_project(fdl_project_dir, publish_url=publish_url)
        # Add S3 credentials on [data] so publish_s3_config falls back to them
        (fdl_project_dir / "fdl.toml").write_text(
            (fdl_project_dir / "fdl.toml").read_text()
            + "\n"
            "[data.credentials_hack]\n"  # placeholder — overwrite below
        )
        # Re-write cleanly with S3 creds in [data]
        sqlite_path = fdl_project_dir / FDL_DIR / "ducklake.sqlite"
        data_dir = fdl_project_dir / FDL_DIR / "data"
        (fdl_project_dir / "fdl.toml").write_text(
            f'name = "ds"\n\n'
            f"[metadata]\n"
            f'url = "sqlite:///{sqlite_path.resolve()}"\n\n'
            f"[data]\n"
            f'url = "{data_dir.resolve()}"\n'
            f's3_endpoint = "https://s3.us-east-1.amazonaws.com"\n'
            f's3_access_key_id = "testing"\n'
            f's3_secret_access_key = "testing"\n\n'
            f"[publishes.default]\n"
            f'url = "{publish_url}"\n'
        )

        fdl.publish(project_dir=fdl_project_dir)

        # Objects present
        toml_obj = moto_s3.get_object(Bucket=BUCKET, Key="myds/fdl.toml")
        assert toml_obj["ContentType"].startswith("application/toml")
        duckdb_obj = moto_s3.get_object(Bucket=BUCKET, Key=f"myds/{DUCKLAKE_FILE}")
        assert duckdb_obj["ContentLength"] > 0

        # ETag state recorded
        state_path = fdl_project_dir / FDL_DIR / "publishes" / "default" / META_JSON
        state = json.loads(state_path.read_text())
        assert state["remote_etag"] == duckdb_obj["ETag"]

    def test_s3_precondition_conflict_detected(
        self, fdl_project_dir, tmp_path, moto_s3
    ):
        publish_url = f"s3://{BUCKET}/myds"
        sqlite_path = fdl_project_dir / FDL_DIR / "ducklake.sqlite"
        data_dir = fdl_project_dir / FDL_DIR / "data"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        (fdl_project_dir / "fdl.toml").write_text(
            f'name = "ds"\n\n'
            f"[metadata]\n"
            f'url = "sqlite:///{sqlite_path.resolve()}"\n\n'
            f"[data]\n"
            f'url = "{data_dir.resolve()}"\n'
            f's3_endpoint = "https://s3.us-east-1.amazonaws.com"\n'
            f's3_access_key_id = "testing"\n'
            f's3_secret_access_key = "testing"\n\n'
            f"[publishes.default]\n"
            f'url = "{publish_url}"\n'
        )
        spec = CatalogSpec(
            scheme="sqlite",
            raw=f"sqlite:///{sqlite_path}",
            path=str(sqlite_path),
        )
        init_ducklake(spec, str(data_dir), "ds")

        # First publish succeeds
        fdl.publish(project_dir=fdl_project_dir)

        # Simulate a concurrent publisher overwriting the catalog without
        # updating our local ETag state.
        moto_s3.put_object(
            Bucket=BUCKET,
            Key=f"myds/{DUCKLAKE_FILE}",
            Body=b"not a real duckdb",
        )

        # Second publish from our process must detect the conflict.
        with pytest.raises(PushConflictError):
            fdl.publish(project_dir=fdl_project_dir)

    def test_s3_force_bypasses_conflict(
        self, fdl_project_dir, tmp_path, moto_s3
    ):
        publish_url = f"s3://{BUCKET}/myds"
        sqlite_path = fdl_project_dir / FDL_DIR / "ducklake.sqlite"
        data_dir = fdl_project_dir / FDL_DIR / "data"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        (fdl_project_dir / "fdl.toml").write_text(
            f'name = "ds"\n\n'
            f"[metadata]\n"
            f'url = "sqlite:///{sqlite_path.resolve()}"\n\n'
            f"[data]\n"
            f'url = "{data_dir.resolve()}"\n'
            f's3_endpoint = "https://s3.us-east-1.amazonaws.com"\n'
            f's3_access_key_id = "testing"\n'
            f's3_secret_access_key = "testing"\n\n'
            f"[publishes.default]\n"
            f'url = "{publish_url}"\n'
        )
        spec = CatalogSpec(
            scheme="sqlite",
            raw=f"sqlite:///{sqlite_path}",
            path=str(sqlite_path),
        )
        init_ducklake(spec, str(data_dir), "ds")

        fdl.publish(project_dir=fdl_project_dir)
        moto_s3.put_object(
            Bucket=BUCKET,
            Key=f"myds/{DUCKLAKE_FILE}",
            Body=b"interloper",
        )
        # force=True should succeed despite the mismatch.
        fdl.publish(project_dir=fdl_project_dir, force=True)
