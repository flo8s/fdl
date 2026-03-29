"""Tests for fdl/s3.py — S3Config.endpoint_host.

DuckDB s3_endpoint にはスキームなしのホスト名を渡す必要がある。
endpoint_host はスキームを除去する変換プロパティ。
スキーム付き・なし・空文字のいずれでも正しく動くことを検証する。
"""

from fdl.s3 import S3Config


def test_endpoint_host_https():
    """https:// prefix is stripped."""
    c = S3Config(bucket="b", endpoint="https://abc.r2.dev", access_key_id="k", secret_access_key="s")
    assert c.endpoint_host == "abc.r2.dev"


def test_endpoint_host_http():
    """http:// prefix is stripped."""
    c = S3Config(bucket="b", endpoint="http://abc.r2.dev", access_key_id="k", secret_access_key="s")
    assert c.endpoint_host == "abc.r2.dev"


def test_endpoint_host_bare():
    """Bare hostname passes through unchanged."""
    c = S3Config(bucket="b", endpoint="abc.r2.dev", access_key_id="k", secret_access_key="s")
    assert c.endpoint_host == "abc.r2.dev"


def test_endpoint_host_empty():
    """Empty endpoint (local target) returns empty string."""
    c = S3Config(bucket="b", endpoint="", access_key_id="k", secret_access_key="s")
    assert c.endpoint_host == ""
