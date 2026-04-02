"""Push: upload DuckLake catalog artifacts to S3 or local directory."""

from __future__ import annotations

import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, FDL_DIR, META_JSON
from fdl.config import PROJECT_CONFIG
from fdl.console import console


def push_to_local(
    output_dir: Path, dist_dir: Path, datasource: str, project_dir: Path,
    *, force: bool = False, target_name: str,
) -> None:
    """Copy artifacts to a local directory."""
    from fdl.meta import check_conflict, read_pushed_at, stamp, write_meta

    remote_meta = output_dir / datasource / FDL_DIR / META_JSON
    check_conflict(read_pushed_at(remote_meta), force=force, target_name=target_name)

    dest = output_dir / datasource
    dest.mkdir(parents=True, exist_ok=True)

    for name in [DUCKLAKE_FILE, DUCKLAKE_SQLITE]:
        src = dist_dir / name
        if src.exists():
            console.print(f"  [dim]{datasource}/{name}[/dim]")
            shutil.copy2(src, dest / name)

    # fdl.toml
    toml_src = project_dir / PROJECT_CONFIG
    if toml_src.exists():
        console.print(f"  [dim]{datasource}/{PROJECT_CONFIG}[/dim]")
        shutil.copy2(toml_src, dest / PROJECT_CONFIG)

    # .fdl/meta.json (write after catalog for fail-safe ordering)
    from fdl import fdl_target_dir

    pushed_at = stamp()
    write_meta(dest / FDL_DIR / META_JSON, pushed_at)
    write_meta(fdl_target_dir(target_name) / META_JSON, pushed_at)


def _upload(
    client,
    bucket: str,
    key: str,
    file_path: Path,
    content_type: str | None = None,
    cache_control: str | None = None,
) -> None:
    """Upload a single file to S3."""
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if cache_control:
        extra_args["CacheControl"] = cache_control

    console.print(f"  [dim]{key}[/dim]")
    client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args or None)


def _upload_if_exists(
    client,
    bucket: str,
    key: str,
    file_path: Path,
    content_type: str | None = None,
    cache_control: str | None = None,
) -> None:
    """Upload a file to S3 only if it exists locally."""
    if file_path.exists():
        _upload(client, bucket, key, file_path, content_type, cache_control)


def push_to_s3(
    client, bucket: str, dist_dir: Path, datasource: str, project_dir: Path,
    *, force: bool = False, target_name: str,
) -> None:
    """Upload artifacts to S3."""
    import json

    from fdl.meta import check_conflict, read_pushed_at_s3, stamp, write_meta

    check_conflict(read_pushed_at_s3(client, bucket, datasource), force=force, target_name=target_name)

    _upload(
        client,
        bucket,
        f"{datasource}/{DUCKLAKE_FILE}",
        dist_dir / DUCKLAKE_FILE,
        cache_control="no-cache",
    )

    _upload_if_exists(
        client,
        bucket,
        f"{datasource}/{DUCKLAKE_SQLITE}",
        dist_dir / DUCKLAKE_SQLITE,
    )

    # fdl.toml
    _upload_if_exists(
        client,
        bucket,
        f"{datasource}/{PROJECT_CONFIG}",
        project_dir / PROJECT_CONFIG,
        content_type="application/toml; charset=utf-8",
    )

    # .fdl/meta.json (upload after catalog for fail-safe ordering)
    from fdl import fdl_target_dir

    pushed_at = stamp()
    client.put_object(
        Bucket=bucket,
        Key=f"{datasource}/{FDL_DIR}/{META_JSON}",
        Body=json.dumps({"pushed_at": pushed_at}).encode(),
        ContentType="application/json; charset=utf-8",
    )
    write_meta(fdl_target_dir(target_name) / META_JSON, pushed_at)
