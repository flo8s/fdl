"""Push: upload DuckLake catalog artifacts to S3 or local directory."""

import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE
from fdl.config import PROJECT_CONFIG
from fdl.console import console


def push_to_local(
    output_dir: Path, dist_dir: Path, datasource: str, project_dir: Path
) -> None:
    """Copy artifacts to a local directory."""
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
    client, bucket: str, dist_dir: Path, datasource: str, project_dir: Path
) -> None:
    """Upload artifacts to S3."""

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
