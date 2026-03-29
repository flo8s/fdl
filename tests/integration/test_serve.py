"""Integration tests for fdl serve.

Spec (docs/reference/cli.md#serve):
  fdl serve TARGET [--port PORT]
  - Serves target directory over HTTP (CORS + Range support)
  - Resolves target from fdl.toml
"""

import threading
import urllib.request
from functools import partial
from http.server import HTTPServer
from pathlib import Path

from typer.testing import CliRunner

from fdl.cli import app
from fdl.serve import CORSRangeHandler


def _start_server(directory: Path):
    handler = partial(CORSRangeHandler, directory=str(directory))
    server = HTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def test_pushed_catalog_is_served(fdl_project_dir: Path):
    """After push, the catalog is accessible over HTTP."""
    storage = fdl_project_dir / "storage"
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(storage),
        "--target-name", "default",
    ])
    cli.invoke(app, ["push", "default"])

    # Serve the target directory (same dir that fdl serve would resolve)
    server, base = _start_server(storage)
    try:
        resp = urllib.request.urlopen(f"{base}/test_ds/ducklake.duckdb")
        assert resp.status == 200
        assert len(resp.read()) > 0
        assert resp.headers["Access-Control-Allow-Origin"] == "*"
    finally:
        server.shutdown()


def test_without_init_fails(fdl_project_dir: Path):
    """fdl serve fails when fdl.toml does not exist."""
    result = CliRunner().invoke(app, ["serve", "default"])
    assert result.exit_code != 0


def test_unknown_target_fails(fdl_project_dir: Path):
    """fdl serve with an unregistered target name fails."""
    cli = CliRunner()
    cli.invoke(app, [
        "init", "test_ds",
        "--public-url", "http://localhost:4001",
        "--target-url", str(fdl_project_dir / "storage"),
        "--target-name", "default",
    ])

    result = cli.invoke(app, ["serve", "nonexistent"])
    assert result.exit_code != 0
