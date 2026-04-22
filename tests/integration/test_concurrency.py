"""Concurrent read/write tests for the local SQLite catalog.

DuckDB file locking rejects a second opener with a LockError. SQLite
uses OS-level locking with snapshot isolation, so multiple processes
can attach the same catalog file simultaneously. These tests pin the
v0.9 behavior.
"""

import subprocess
import sys
from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app


def _init_and_seed(project_dir: Path) -> None:
    cli = CliRunner()
    cli.invoke(
        app,
        [
            "init",
            "test_ds",
            "--public-url",
            "http://localhost:4001",
            "--target-url",
            str(project_dir / "storage"),
            "--target-name",
            "default",
        ],
    )
    cli.invoke(app, ["sql", "default", "CREATE TABLE t (x INTEGER)"])
    cli.invoke(app, ["sql", "default", "INSERT INTO t VALUES (1)"])


def _fdl(args: list[str], cwd: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "fdl.cli", *args],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_two_readers_can_attach_concurrently(fdl_project_dir: Path):
    """Two ``fdl sql SELECT`` processes can run simultaneously without errors."""
    _init_and_seed(fdl_project_dir)

    p1 = _fdl(["sql", "default", "SELECT COUNT(*) FROM t"], fdl_project_dir)
    p2 = _fdl(["sql", "default", "SELECT COUNT(*) FROM t"], fdl_project_dir)

    rc1 = p1.wait(timeout=60)
    rc2 = p2.wait(timeout=60)
    assert rc1 == 0, p1.stderr.read().decode()
    assert rc2 == 0, p2.stderr.read().decode()


def test_writer_and_reader_do_not_deadlock(fdl_project_dir: Path):
    """A concurrent writer and reader both complete within the timeout."""
    _init_and_seed(fdl_project_dir)

    writer = _fdl(["sql", "default", "INSERT INTO t VALUES (2)"], fdl_project_dir)
    reader = _fdl(["sql", "default", "SELECT COUNT(*) FROM t"], fdl_project_dir)

    rc_w = writer.wait(timeout=60)
    rc_r = reader.wait(timeout=60)
    assert rc_w == 0, writer.stderr.read().decode()
    assert rc_r == 0, reader.stderr.read().decode()
