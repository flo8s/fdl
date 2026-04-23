"""Integration tests for fdl pull (sqlite re-sync from publish destination)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import fdl
from fdl import DUCKLAKE_FILE, FDL_DIR
from fdl.config import CatalogSpec, metadata_spec
from fdl.ducklake import build_attach_sql, init_ducklake


def _setup(project_dir: Path, *, publish_to: Path) -> tuple[Path, Path]:
    """Initialize a sqlite project with one publish destination."""
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
        f'url = "{publish_to}"\n'
    )
    spec = CatalogSpec(
        scheme="sqlite", raw=f"sqlite:///{sqlite_path}", path=str(sqlite_path)
    )
    init_ducklake(spec, str(data_dir), "ds")
    return sqlite_path, data_dir


def _insert_row(sqlite_path: Path, data_dir: Path, value: int) -> None:
    spec = CatalogSpec(
        scheme="sqlite", raw=f"sqlite:///{sqlite_path}", path=str(sqlite_path)
    )
    conn = duckdb.connect()
    try:
        for stmt in build_attach_sql(
            metadata=spec, data_url=str(data_dir), datasource="ds"
        ):
            conn.execute(stmt)
        conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        conn.execute(f"INSERT INTO t VALUES ({value})")
    finally:
        conn.close()


class TestPullSqlite:
    def test_pull_refetches_from_publish(self, fdl_project_dir, tmp_path):
        publish_to = tmp_path / "pub"
        sqlite_path, data_dir = _setup(fdl_project_dir, publish_to=publish_to)

        # Write some data and publish it.
        _insert_row(sqlite_path, data_dir, 1)
        fdl.publish(project_dir=fdl_project_dir)

        # Corrupt the local live catalog (simulate drift) and pull.
        sqlite_path.write_bytes(b"not a sqlite database")
        fdl.pull(project_dir=fdl_project_dir)

        # Post-pull the local catalog is valid again and sees row 1.
        spec = CatalogSpec(
            scheme="sqlite", raw=f"sqlite:///{sqlite_path}", path=str(sqlite_path),
        )
        conn = duckdb.connect()
        try:
            for s in build_attach_sql(
                metadata=spec, data_url=str(data_dir), datasource="ds",
                read_only=True,
            ):
                conn.execute(s)
            assert conn.execute("SELECT x FROM t").fetchone() == (1,)
        finally:
            conn.close()

    def test_pull_without_publishes_errors(self, fdl_project_dir, tmp_path):
        sqlite_path = fdl_project_dir / FDL_DIR / "ducklake.sqlite"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        (fdl_project_dir / "fdl.toml").write_text(
            f'name = "ds"\n'
            f"[metadata]\n"
            f'url = "sqlite:///{sqlite_path.resolve()}"\n'
            f"[data]\n"
            f'url = "{fdl_project_dir}/data"\n'
        )
        with pytest.raises(KeyError):
            fdl.pull(project_dir=fdl_project_dir)

    def test_pull_rejects_postgres_metadata(self, fdl_project_dir, tmp_path):
        (fdl_project_dir / "fdl.toml").write_text(
            'name = "ds"\n'
            "[metadata]\n"
            'url = "postgres://h/db"\n'
            "[data]\n"
            f'url = "{fdl_project_dir}/data"\n'
            "[publishes.default]\n"
            f'url = "{tmp_path / "pub"}"\n'
        )
        with pytest.raises(ValueError, match="only supported for sqlite"):
            fdl.pull(project_dir=fdl_project_dir)
