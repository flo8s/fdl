"""Tests for CORSRangeHandler — CORS headers and Range request support.

DuckDB WASM requires CORS and Range requests to ATTACH a remote DuckLake catalog.
These tests verify the HTTP handler behavior independently of fdl commands.
"""

import threading
import urllib.error
import urllib.request
from functools import partial
from http.server import HTTPServer
from pathlib import Path

from fdl.serve import CORSRangeHandler

TEST_CONTENT = b"Hello, DuckLake! " * 100  # 1700 bytes


def _start_server(directory: Path):
    handler = partial(CORSRangeHandler, directory=str(directory))
    server = HTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


def test_get_returns_file(tmp_path: Path):
    (tmp_path / "data.bin").write_bytes(TEST_CONTENT)
    server, base = _start_server(tmp_path)
    try:
        resp = urllib.request.urlopen(f"{base}/data.bin")
        assert resp.read() == TEST_CONTENT
        assert resp.status == 200
    finally:
        server.shutdown()


def test_cors_headers(tmp_path: Path):
    (tmp_path / "data.bin").write_bytes(TEST_CONTENT)
    server, base = _start_server(tmp_path)
    try:
        resp = urllib.request.urlopen(f"{base}/data.bin")
        assert resp.headers["Access-Control-Allow-Origin"] == "*"
    finally:
        server.shutdown()


def test_range_request(tmp_path: Path):
    (tmp_path / "data.bin").write_bytes(TEST_CONTENT)
    server, base = _start_server(tmp_path)
    try:
        req = urllib.request.Request(f"{base}/data.bin", headers={"Range": "bytes=0-9"})
        resp = urllib.request.urlopen(req)
        assert resp.status == 206
        assert resp.read() == TEST_CONTENT[:10]
        assert resp.headers["Content-Range"] == f"bytes 0-9/{len(TEST_CONTENT)}"
    finally:
        server.shutdown()


def test_head_returns_size_and_accept_ranges(tmp_path: Path):
    (tmp_path / "data.bin").write_bytes(TEST_CONTENT)
    server, base = _start_server(tmp_path)
    try:
        req = urllib.request.Request(f"{base}/data.bin", method="HEAD")
        resp = urllib.request.urlopen(req)
        assert resp.headers["Content-Length"] == str(len(TEST_CONTENT))
        assert resp.headers["Accept-Ranges"] == "bytes"
    finally:
        server.shutdown()


def test_options_returns_204(tmp_path: Path):
    server, base = _start_server(tmp_path)
    try:
        req = urllib.request.Request(f"{base}/", method="OPTIONS")
        resp = urllib.request.urlopen(req)
        assert resp.status == 204
    finally:
        server.shutdown()


def test_missing_file_returns_404(tmp_path: Path):
    server, base = _start_server(tmp_path)
    try:
        try:
            urllib.request.urlopen(f"{base}/no_such_file.bin")
            assert False, "Expected HTTPError"
        except urllib.error.HTTPError as e:
            assert e.code == 404
    finally:
        server.shutdown()
