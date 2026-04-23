"""Pull: rebuild the local SQLite live catalog from a publish target."""

from __future__ import annotations

import shutil
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

from fdl import DUCKLAKE_FILE
from fdl.console import console


def pull(
    name: str | None = None,
    *,
    project_dir: Path | None = None,
) -> None:
    """Rebuild the local SQLite live catalog from a publish target.

    Reads ``[publishes.<name>]`` from the local fdl.toml, fetches the frozen
    ``ducklake.duckdb`` from that URL, and converts it into the SQLite file
    referenced by ``[metadata].url``, overwriting any existing catalog.

    SQLite-only. When ``[metadata].url`` points at PostgreSQL, pull refuses
    because PostgreSQL lives catalogs are the source of truth and are always
    up-to-date by design.
    """
    from fdl.config import (
        data_url,
        datasource_name,
        find_project_dir,
        metadata_spec,
        publish_s3_config,
        publish_url,
        resolve_publish_name,
    )
    from fdl.ducklake import _convert_ducklake_catalog

    root = project_dir or find_project_dir()
    spec = metadata_spec(root)

    if spec.scheme != "sqlite":
        raise ValueError(
            "'fdl pull' is only applicable for SQLite metadata. "
            "PostgreSQL is always up-to-date by design."
        )
    if spec.path is None:
        raise ValueError("sqlite metadata spec missing path")

    resolved_name = resolve_publish_name(name, root)
    base = _normalize_base(publish_url(resolved_name, root))
    local_sqlite = Path(spec.path)
    local_data_url = data_url(root)
    datasource = datasource_name(root)

    console.print(f"[bold]--- pull: {datasource} ← {base} ---[/bold]")

    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".duckdb")
    import os

    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        console.print(f"  [dim]{DUCKLAKE_FILE}[/dim]")
        _fetch(
            base + DUCKLAKE_FILE,
            tmp_path,
            s3_config=publish_s3_config(resolved_name, root),
        )

        local_sqlite.parent.mkdir(parents=True, exist_ok=True)
        if local_sqlite.exists():
            local_sqlite.unlink()
        _convert_ducklake_catalog(
            tmp_path,
            local_sqlite,
            src_type="duckdb",
            dst_type="sqlite",
            data_path=local_data_url,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    console.print(f"[green]Pulled {datasource} from {base}[/green]")


def _normalize_base(url: str) -> str:
    """Ensure the base URL ends with ``/``."""
    return url if url.endswith("/") else url + "/"


def _fetch(url: str, dest: Path, *, s3_config: object | None = None) -> None:
    """Fetch ``url`` into ``dest``. Supports http(s), s3, file, local paths."""
    scheme = urllib.parse.urlsplit(url).scheme
    if scheme in ("http", "https"):
        with urllib.request.urlopen(url) as resp, dest.open("wb") as f:
            shutil.copyfileobj(resp, f)
        return
    if scheme == "s3":
        _fetch_s3(url, dest, s3_config)
        return
    if scheme in ("", "file"):
        local = urllib.parse.urlsplit(url).path if scheme == "file" else url
        src = Path(local)
        if not src.exists():
            raise FileNotFoundError(src)
        shutil.copy2(src, dest)
        return
    raise ValueError(f"Unsupported URL scheme for pull: {scheme!r}")


def _fetch_s3(url: str, dest: Path, s3_config: object | None) -> None:
    from fdl.s3 import S3Config, create_s3_client

    rest = url.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    if not key:
        raise ValueError(f"S3 URL missing key: {url!r}")
    if isinstance(s3_config, S3Config):
        client = create_s3_client(s3_config)
    else:
        import boto3

        client = boto3.client("s3")
    client.download_file(bucket, key, str(dest))
