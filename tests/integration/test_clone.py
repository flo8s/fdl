"""Integration tests for fdl clone."""

from __future__ import annotations

import tomllib
from pathlib import Path

import duckdb
import pytest

import fdl
from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, FDL_DIR
from fdl.config import (
    CatalogSpec,
    data_url,
    metadata_spec,
    metadata_url,
)
from fdl.ducklake import (
    _convert_ducklake_catalog,
    build_attach_sql,
    init_ducklake,
)


def _build_published_dir(
    tmp_path: Path,
    *,
    name: str = "ds",
) -> tuple[Path, str]:
    """Build a minimal published frozen DuckLake under tmp_path.

    Returns (published_dir, data_url).
    """
    # Build a sqlite live catalog
    live_dir = tmp_path / "live"
    live_dir.mkdir()
    sqlite_path = live_dir / "ducklake.sqlite"
    data_dir = live_dir / "data"

    spec = CatalogSpec(
        scheme="sqlite",
        raw=f"sqlite:///{sqlite_path}",
        path=str(sqlite_path),
    )
    init_ducklake(spec, str(data_dir), name)

    # Populate with a row
    conn = duckdb.connect()
    try:
        for s in build_attach_sql(
            metadata=spec,
            data_url=str(data_dir),
            datasource=name,
        ):
            conn.execute(s)
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (42)")
    finally:
        conn.close()

    # Publish: sqlite -> duckdb with an absolute data_url baked in
    pub_dir = tmp_path / "pub"
    pub_dir.mkdir()
    pub_data_url = str(data_dir.resolve())
    _convert_ducklake_catalog(
        sqlite_path,
        pub_dir / DUCKLAKE_FILE,
        src_type="sqlite",
        dst_type="duckdb",
        data_path=pub_data_url,
    )

    # Write the accompanying fdl.toml
    (pub_dir / "fdl.toml").write_text(
        f'name = "{name}"\n\n[data]\nurl = "{pub_data_url}"\n'
    )
    return pub_dir, pub_data_url


class TestCloneLocal:
    def test_round_trip_sqlite(self, tmp_path, fdl_project_dir):
        pub_dir, pub_data_url = _build_published_dir(tmp_path, name="myds")

        fdl.clone(str(pub_dir), project_dir=fdl_project_dir)

        # Config file exists with v0.11 schema
        toml_text = (fdl_project_dir / "fdl.toml").read_text()
        data = tomllib.loads(toml_text)
        assert data["name"] == "myds"
        assert data["metadata"]["url"].startswith("sqlite:///")
        assert data["data"]["url"] == pub_data_url

        # Local sqlite catalog exists
        assert (fdl_project_dir / FDL_DIR / DUCKLAKE_SQLITE).exists()

        # Helpers pick up the cloned config
        assert metadata_spec(fdl_project_dir).scheme == "sqlite"
        assert data_url(fdl_project_dir) == pub_data_url
        assert metadata_url(fdl_project_dir).startswith("sqlite:///")

        # The cloned sqlite contains the row
        spec = metadata_spec(fdl_project_dir)
        conn = duckdb.connect()
        try:
            for s in build_attach_sql(
                metadata=spec, data_url=pub_data_url, datasource="myds",
                read_only=True,
            ):
                conn.execute(s)
            assert conn.execute("SELECT x FROM t").fetchone() == (42,)
        finally:
            conn.close()

    def test_refuses_overwrite_without_force(self, tmp_path, fdl_project_dir):
        pub_dir, _ = _build_published_dir(tmp_path)
        (fdl_project_dir / "fdl.toml").write_text('name = "existing"\n')
        with pytest.raises(FileExistsError):
            fdl.clone(str(pub_dir), project_dir=fdl_project_dir)

    def test_force_overwrites(self, tmp_path, fdl_project_dir):
        pub_dir, _ = _build_published_dir(tmp_path, name="newds")
        (fdl_project_dir / "fdl.toml").write_text('name = "old"\n')
        fdl.clone(str(pub_dir), project_dir=fdl_project_dir, force=True)
        data = tomllib.loads((fdl_project_dir / "fdl.toml").read_text())
        assert data["name"] == "newds"

    def test_missing_fdl_toml_errors(self, tmp_path, fdl_project_dir):
        empty_pub = tmp_path / "empty"
        empty_pub.mkdir()
        with pytest.raises(FileNotFoundError):
            fdl.clone(str(empty_pub), project_dir=fdl_project_dir)
