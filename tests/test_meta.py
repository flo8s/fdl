"""Unit tests for fdl/meta.py — conflict detection logic.

is_stale is a pure function that determines if the local catalog
is behind the remote based on pushed_at timestamps.
"""

from fdl.meta import is_stale


def test_remote_newer_is_stale():
    """Remote pushed after local → stale."""
    assert is_stale("2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00") is True


def test_same_timestamp_is_not_stale():
    """Timestamps match → not stale (same push, no conflict)."""
    assert is_stale("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00") is False


def test_local_newer_is_not_stale():
    """Local pushed after remote → not stale (e.g. after --force push)."""
    assert is_stale("2026-02-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00") is False


def test_no_local_timestamp_is_not_stale():
    """No local meta.json → not stale (first interaction, no conflict detection)."""
    assert is_stale(None, "2026-01-01T00:00:00+00:00") is False


def test_no_remote_timestamp_is_not_stale():
    """No remote meta.json → not stale (first push to this target)."""
    assert is_stale("2026-01-01T00:00:00+00:00", None) is False


def test_both_none_is_not_stale():
    """Neither side has meta.json → not stale."""
    assert is_stale(None, None) is False
