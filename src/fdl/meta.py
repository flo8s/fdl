"""Push conflict detection via ETag + If-Match preconditions.

Local state (``.fdl/<target>/meta.json``) stores the ETag of the remote
``ducklake.duckdb`` observed after the last push or pull. On the next push,
``put_object`` is issued with ``If-Match: <saved_etag>`` so that S3 rejects
the write with HTTP 412 if another client has pushed in the meantime.
"""

from __future__ import annotations

import json
from pathlib import Path


class PushConflictError(Exception):
    """Raised when remote has been updated since last pull."""


def read_remote_etag(path: Path) -> str | None:
    """Read the saved remote ETag from ``.fdl/<target>/meta.json``.

    Returns None when the file is missing or only contains legacy fields
    (e.g. ``pushed_at`` from pre-ETag versions). In that case the caller
    should treat the state as "no record" and either pull to initialize
    it or use ``--force``.
    """
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    etag = data.get("remote_etag")
    return etag if isinstance(etag, str) else None


def write_remote_etag(path: Path, etag: str) -> None:
    """Write ``{"remote_etag": <etag>}`` to the state file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"remote_etag": etag}))


def catalog_is_stale(
    target: str,
    resolved: str,
    datasource: str,
    *,
    project_dir: Path | None = None,
) -> bool:
    """Return True if the local catalog ETag differs from the remote.

    Used by commands that read from the local catalog (e.g. ``fdl sql``)
    to detect that the local copy is behind the remote.

    Returns False for non-S3 targets (local targets skip conflict
    detection) or when no local ETag has been recorded yet.
    """
    if not resolved.startswith("s3://"):
        return False

    from fdl import DUCKLAKE_FILE, META_JSON, fdl_target_dir
    from fdl.config import find_project_dir, target_s3_config
    from fdl.pull import _head_catalog_etag
    from fdl.s3 import create_s3_client

    root = project_dir or find_project_dir()
    local_etag = read_remote_etag(root / fdl_target_dir(target) / META_JSON)
    if local_etag is None:
        return False

    s3 = target_s3_config(target, root)
    remote_etag = _head_catalog_etag(
        create_s3_client(s3), s3.bucket, f"{datasource}/{DUCKLAKE_FILE}"
    )
    return remote_etag is not None and remote_etag != local_etag
