"""Publish conflict detection via ETag + If-Match preconditions.

Local state (``.fdl/publishes/<name>/meta.json``) stores the ETag of the
remote ``ducklake.duckdb`` observed after the last successful publish. On
the next publish, ``put_object`` is issued with ``If-Match: <saved_etag>``
so that S3 rejects the write with HTTP 412 if another client has published
in the meantime.
"""

from __future__ import annotations

import json
from pathlib import Path


class PushConflictError(Exception):
    """Raised when the remote has been updated since the last publish."""


def read_remote_etag(path: Path) -> str | None:
    """Read the saved remote ETag from the state file."""
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    etag = data.get("remote_etag")
    return etag if isinstance(etag, str) else None


def write_remote_etag(path: Path, etag: str) -> None:
    """Write ``{"remote_etag": <etag>}`` to the state file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"remote_etag": etag}))
