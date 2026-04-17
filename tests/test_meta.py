"""Unit tests for fdl/meta.py — ETag state file I/O."""

import json

from fdl.meta import read_remote_etag, write_remote_etag


def test_read_returns_none_when_missing(tmp_path):
    """No state file → None."""
    assert read_remote_etag(tmp_path / "missing.json") is None


def test_write_then_read_round_trip(tmp_path):
    """Written ETag is read back as-is (quotes preserved)."""
    path = tmp_path / "meta.json"
    write_remote_etag(path, '"abc123"')
    assert read_remote_etag(path) == '"abc123"'


def test_write_creates_parent_directories(tmp_path):
    """write_remote_etag creates missing parent directories."""
    path = tmp_path / "nested" / "dir" / "meta.json"
    write_remote_etag(path, '"xyz"')
    assert path.exists()
    assert json.loads(path.read_text()) == {"remote_etag": '"xyz"'}


def test_read_returns_none_for_legacy_pushed_at(tmp_path):
    """Old {"pushed_at": "..."} state is treated as no record."""
    path = tmp_path / "meta.json"
    path.write_text(json.dumps({"pushed_at": "2026-01-01T00:00:00+00:00"}))
    assert read_remote_etag(path) is None


def test_read_returns_none_when_etag_is_not_string(tmp_path):
    """Malformed remote_etag field is treated as no record."""
    path = tmp_path / "meta.json"
    path.write_text(json.dumps({"remote_etag": None}))
    assert read_remote_etag(path) is None
