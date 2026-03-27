"""Pull: download DuckLake catalog from S3 or local directory."""

import shutil
from pathlib import Path

from botocore.exceptions import ClientError

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, METADATA_JSON
from fdl.console import console


def pull_from_local(source_dir: Path, dist_dir: Path, datasource: str) -> bool:
    """Copy catalog from a local directory into dist/.

    Returns True if catalog was found.
    """
    src = source_dir / datasource
    if not src.exists():
        return False

    dist_dir.mkdir(parents=True, exist_ok=True)

    for name in [DUCKLAKE_FILE, DUCKLAKE_SQLITE, METADATA_JSON]:
        src_file = src / name
        if src_file.exists():
            console.print(f"  [dim]{datasource}/{name}[/dim]")
            shutil.copy2(src_file, dist_dir / name)

    return True


def _download_file(client, bucket: str, key: str, dest: Path) -> bool:
    """Download a single file. Returns True if successful, False if 404."""
    try:
        console.print(f"  [dim]{key}[/dim]")
        client.download_file(bucket, key, str(dest))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            console.print(f"  [yellow]{key} not found, skipping[/yellow]")
            return False
        raise


def fetch_from_s3(client, bucket: str, dist_dir: Path, datasource: str) -> bool:
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
    _download_file(
        client, bucket, f"{datasource}/{METADATA_JSON}", dist_dir / METADATA_JSON
    )

    return found
