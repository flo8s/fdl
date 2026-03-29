"""Push conflict detection via .fdl/meta.json."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from fdl import FDL_DIR, META_JSON
from fdl.console import console


class PushConflictError(Exception):
    """Raised when remote has been updated since last pull."""


def read_pushed_at(path: Path) -> str | None:
    """Read pushed_at from a meta.json file."""
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return data.get("pushed_at")


def read_pushed_at_s3(client, bucket: str, datasource: str) -> str | None:
    """Read pushed_at from remote .fdl/meta.json on S3."""
    from botocore.exceptions import ClientError

    key = f"{datasource}/{FDL_DIR}/{META_JSON}"
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
        return data.get("pushed_at")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def read_remote_pushed_at(resolved: str, target_name: str, datasource: str) -> str | None:
    """Read pushed_at from remote meta.json (S3 or local)."""
    if resolved.startswith("s3://"):
        from fdl.config import target_s3_config
        from fdl.s3 import create_s3_client

        s3 = target_s3_config(target_name)
        return read_pushed_at_s3(create_s3_client(s3), s3.bucket, datasource)
    else:
        return read_pushed_at(Path(resolved) / datasource / FDL_DIR / META_JSON)


def is_stale(local_pushed_at: str | None, remote_pushed_at: str | None) -> bool:
    """Check if local catalog is behind remote.

    Pure function: returns True if remote is newer than local.
    Returns False if either side has no timestamp (first push, or
    pre-conflict-detection state).
    """
    if local_pushed_at is None or remote_pushed_at is None:
        return False
    return remote_pushed_at > local_pushed_at


def check_conflict(remote_pushed_at: str | None, *, force: bool) -> None:
    """Compare remote pushed_at with local record. Raise on conflict."""
    local_pushed_at = read_pushed_at(FDL_DIR / META_JSON)

    if not is_stale(local_pushed_at, remote_pushed_at):
        return

    if force:
        console.print("[yellow]Force: overriding conflict detection[/yellow]")
        return
    raise PushConflictError(
        f"Remote was pushed at {remote_pushed_at}. "
        f"Run 'fdl pull' first, or use --force to override."
    )


def write_meta(path: Path, pushed_at: str) -> None:
    """Write meta.json to a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pushed_at": pushed_at}))


def sync_meta(pushed_at: str | None) -> None:
    """Write or clear local .fdl/meta.json to match remote state.

    When remote has no meta.json (pre-conflict-detection push or never pushed),
    the local copy is removed so both sides are in the "no conflict detection"
    state. The next push will create meta.json on both sides.
    """
    path = FDL_DIR / META_JSON
    if pushed_at is None:
        path.unlink(missing_ok=True)
    else:
        path.write_text(json.dumps({"pushed_at": pushed_at}))


def stamp() -> str:
    """Generate a pushed_at timestamp (UTC ISO 8601)."""
    return datetime.now(timezone.utc).isoformat()
