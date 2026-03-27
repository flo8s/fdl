"""Push: upload DuckLake catalog artifacts to S3 or local directory."""

import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, METADATA_JSON
from fdl.console import console


def push_to_local(output_dir: Path, dist_dir: Path, datasource: str) -> None:
    """Copy artifacts to a local directory."""
    dest = output_dir / datasource
    dest.mkdir(parents=True, exist_ok=True)

    for name in [DUCKLAKE_FILE, DUCKLAKE_SQLITE, METADATA_JSON]:
        src = dist_dir / name
        if src.exists():
            console.print(f"  [dim]{datasource}/{name}[/dim]")
            shutil.copy2(src, dest / name)

    # Data files (ducklake.duckdb.files/)
    ducklake_data_dir = f"{DUCKLAKE_FILE}.files"
    data_src = dist_dir / ducklake_data_dir
    if data_src.exists():
        data_dest = dest / ducklake_data_dir
        if data_dest.exists():
            shutil.rmtree(data_dest)
        console.print(f"  [dim]{datasource}/{ducklake_data_dir}/[/dim]")
        shutil.copytree(data_src, data_dest)

    # docs
    docs_src = dist_dir / "docs"
    if docs_src.exists():
        docs_dest = dest / "docs"
        docs_dest.mkdir(exist_ok=True)
        for name in ("index.html", "manifest.json", "catalog.json"):
            src = docs_src / name
            if src.exists():
                console.print(f"  [dim]{datasource}/docs/{name}[/dim]")
                shutil.copy2(src, docs_dest / name)


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


def push_to_s3(client, bucket: str, dist_dir: Path, datasource: str) -> None:
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

    _upload_if_exists(
        client,
        bucket,
        f"{datasource}/{METADATA_JSON}",
        dist_dir / METADATA_JSON,
        content_type="application/json; charset=utf-8",
    )

    docs_dir = dist_dir / "docs"
    if docs_dir.exists():
        for name, ct in [
            ("index.html", "text/html; charset=utf-8"),
            ("manifest.json", "application/json; charset=utf-8"),
            ("catalog.json", "application/json; charset=utf-8"),
        ]:
            _upload_if_exists(
                client,
                bucket,
                f"{datasource}/docs/{name}",
                docs_dir / name,
                content_type=ct,
            )
