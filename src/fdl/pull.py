"""Pull: download DuckLake catalog from S3 or local directory."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, META_JSON
from fdl.console import console
from fdl.meta import remote_meta_key


def do_pull(
    resolved: str,
    target: str,
    dist_dir: Path,
    datasource: str,
    project_dir: Path | None = None,
) -> None:
    """Pull catalog from a target (S3 or local)."""
    if resolved.startswith("s3://"):
        from fdl.config import target_s3_config
        from fdl.s3 import create_s3_client

        s3 = target_s3_config(target, project_dir)
        client = create_s3_client(s3)
        fetch_from_s3(
            client, s3.bucket, dist_dir, datasource,
            target_name=target, project_dir=project_dir,
        )
    else:
        pull_from_local(
            Path(resolved), dist_dir, datasource,
            target_name=target, project_dir=project_dir,
        )


def pull_if_needed(
    target_dir: Path,
    resolved: str,
    target: str,
    datasource: str,
    project_dir: Path | None = None,
) -> str | None:
    """Pull if local catalog is missing, unsynced, or stale.

    Returns the reason for pulling, or None if already up to date.
    """
    if not (target_dir / DUCKLAKE_FILE).exists() and not (target_dir / DUCKLAKE_SQLITE).exists():
        reason = "No local catalog"
    elif not (target_dir / META_JSON).exists():
        reason = "Catalog not synced"
    else:
        from fdl.meta import is_stale, read_pushed_at, read_remote_pushed_at

        local = read_pushed_at(target_dir / META_JSON)
        remote = read_remote_pushed_at(resolved, target, datasource, project_dir)
        if is_stale(local, remote):
            reason = "Remote is newer"
        else:
            return None

    do_pull(resolved, target, target_dir, datasource, project_dir)
    return reason


def pull_from_local(
    source_dir: Path,
    dist_dir: Path,
    datasource: str,
    *,
    target_name: str,
    project_dir: Path | None = None,
) -> bool:
    """Copy catalog from a local directory into dist/.

    Returns True if catalog was found.
    """
    src = source_dir / datasource
    if not src.exists():
        return False

    dist_dir.mkdir(parents=True, exist_ok=True)

    for name in [DUCKLAKE_FILE, DUCKLAKE_SQLITE]:
        src_file = src / name
        if src_file.exists():
            console.print(f"  [dim]{datasource}/{name}[/dim]")
            shutil.copy2(src_file, dist_dir / name)

    # Sync .fdl/meta.json from remote
    from fdl.meta import sync_meta

    meta_file = source_dir / remote_meta_key(datasource)
    if meta_file.exists():
        data = json.loads(meta_file.read_text())
        sync_meta(data.get("pushed_at"), target_name, project_dir)
    else:
        sync_meta(None, target_name, project_dir)

    return True


def _download_file(client, bucket: str, key: str, dest: Path) -> bool:
    """Download a single file. Returns True if successful, False if 404."""
    from botocore.exceptions import ClientError

    try:
        console.print(f"  [dim]{key}[/dim]")
        client.download_file(bucket, key, str(dest))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            console.print(f"  [yellow]{key} not found, skipping[/yellow]")
            return False
        raise


def fetch_from_s3(
    client,
    bucket: str,
    dist_dir: Path,
    datasource: str,
    *,
    target_name: str,
    project_dir: Path | None = None,
) -> bool:
    """Download DuckLake catalog files from S3.

    Returns True if ducklake.duckdb was found (fetch succeeded).
    """
    dist_dir.mkdir(parents=True, exist_ok=True)

    found = _download_file(
        client, bucket, f"{datasource}/{DUCKLAKE_FILE}", dist_dir / DUCKLAKE_FILE
    )
    _download_file(
        client, bucket, f"{datasource}/{DUCKLAKE_SQLITE}", dist_dir / DUCKLAKE_SQLITE
    )

    # Sync pushed_at from remote meta
    from fdl.meta import read_pushed_at_s3, sync_meta

    sync_meta(read_pushed_at_s3(client, bucket, datasource), target_name, project_dir)

    return found
