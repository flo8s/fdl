"""Concurrent read/write behavior of the local DuckLake catalog.

These tests pin the concrete difference between v0.8 (DuckDB catalog,
exclusive file lock) and v0.9 (SQLite catalog, concurrent-friendly).
Both tests drive two subprocesses that ATTACH the same catalog in
lockstep via a ready/go file-based barrier, so the ATTACH time windows
are guaranteed to overlap.
"""

import subprocess
import sys
import time
from pathlib import Path

from typer.testing import CliRunner

from fdl import (
    DUCKLAKE_FILE,
    DUCKLAKE_SQLITE,
    ducklake_data_path,
    fdl_target_dir,
)
from fdl.cli import app
from fdl.ducklake import _convert_ducklake_catalog

_ATTACHER_SCRIPT = """
import sys
import time
from pathlib import Path

ready = Path(sys.argv[1])
go = Path(sys.argv[2])
catalog = sys.argv[3]
kind = sys.argv[4]

import duckdb

conn = duckdb.connect()
conn.execute("INSTALL ducklake; LOAD ducklake;")
if kind == "sqlite":
    conn.execute("INSTALL sqlite; LOAD sqlite;")

ready.touch()
deadline = time.time() + 30.0
while not go.exists():
    if time.time() > deadline:
        sys.stderr.write("timeout waiting for go signal\\n")
        sys.exit(2)
    time.sleep(0.01)

try:
    conn.execute(f"ATTACH 'ducklake:{catalog}' AS ds")
    conn.execute("USE ds")
    # Hold the attach long enough that the peer's ATTACH also lands
    # inside this critical section.
    time.sleep(1.5)
    conn.close()
    sys.exit(0)
except Exception as e:
    sys.stderr.write(f"{type(e).__name__}: {e}\\n")
    sys.exit(1)
"""


def _run_two_attachers(
    catalog: Path, kind: str, tmp_dir: Path
) -> tuple[int, str, int, str]:
    script = tmp_dir / "attacher.py"
    script.write_text(_ATTACHER_SCRIPT)

    ready1 = tmp_dir / "ready1"
    ready2 = tmp_dir / "ready2"
    go = tmp_dir / "go"
    for f in [ready1, ready2, go]:
        f.unlink(missing_ok=True)

    def _spawn(ready: Path) -> subprocess.Popen:
        return subprocess.Popen(
            [
                sys.executable,
                str(script),
                str(ready),
                str(go),
                str(catalog),
                kind,
            ],
            stderr=subprocess.PIPE,
        )

    p1 = _spawn(ready1)
    p2 = _spawn(ready2)

    deadline = time.time() + 30.0
    while not (ready1.exists() and ready2.exists()):
        if time.time() > deadline:
            p1.kill()
            p2.kill()
            raise RuntimeError("attacher processes never became ready")
        time.sleep(0.01)
    go.touch()

    rc1 = p1.wait(timeout=60)
    rc2 = p2.wait(timeout=60)
    err1 = p1.stderr.read().decode()
    err2 = p2.stderr.read().decode()
    return rc1, err1, rc2, err2


def _init(project_dir: Path) -> Path:
    CliRunner().invoke(
        app,
        [
            "init",
            "ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            str(project_dir / "storage"),
            "--target-name",
            "default",
        ],
    )
    return project_dir / fdl_target_dir("default")


def test_sqlite_catalog_allows_concurrent_attach(fdl_project_dir: Path):
    """Two processes ATTACHing the SQLite catalog simultaneously both succeed."""
    dist_dir = _init(fdl_project_dir)
    sqlite_file = dist_dir / DUCKLAKE_SQLITE

    rc1, err1, rc2, err2 = _run_two_attachers(
        sqlite_file, "sqlite", fdl_project_dir
    )
    assert rc1 == 0, f"first attacher failed: {err1}"
    assert rc2 == 0, f"second attacher failed: {err2}"


def test_duckdb_catalog_conflicts_on_concurrent_attach(fdl_project_dir: Path):
    """Two processes ATTACHing a DuckDB catalog simultaneously conflict.

    This pins the v0.8 failure mode (DuckDB's exclusive file lock) that
    motivates the v0.9 switch to SQLite-only local catalogs. If this ever
    stops failing, the justification for the SQLite switch needs revisiting.
    """
    dist_dir = _init(fdl_project_dir)
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    _convert_ducklake_catalog(
        sqlite_file,
        duckdb_file,
        src_type="sqlite",
        dst_type="duckdb",
        data_path=ducklake_data_path(str(duckdb_file)),
    )

    rc1, err1, rc2, err2 = _run_two_attachers(
        duckdb_file, "duckdb", fdl_project_dir
    )
    assert (rc1 != 0) or (rc2 != 0), (
        "expected at least one attacher to fail with a DuckDB lock error, "
        f"but both succeeded (rc1={rc1}, rc2={rc2})"
    )
    combined_err = err1 + err2
    assert "Conflicting lock" in combined_err or "lock" in combined_err.lower(), (
        f"expected a lock-related error, got: {combined_err!r}"
    )
