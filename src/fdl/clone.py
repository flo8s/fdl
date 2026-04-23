"""Clone: initialize a live catalog from a published frozen DuckLake."""

from __future__ import annotations

import shutil
import tempfile
import tomllib
import urllib.parse
import urllib.request
from pathlib import Path

from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, FDL_DIR
from fdl.config import PROJECT_CONFIG
from fdl.console import console


def _fetch(url: str, dest: Path | None = None) -> bytes | None:
    """Fetch URL content. If ``dest`` is given, write to it and return None."""
    scheme = urllib.parse.urlsplit(url).scheme
    if scheme in ("http", "https"):
        return _fetch_https(url, dest)
    if scheme == "s3":
        return _fetch_s3(url, dest)
    if scheme == "file" or scheme == "":
        local_path = url
        if scheme == "file":
            local_path = urllib.parse.urlsplit(url).path
        return _fetch_local(Path(local_path), dest)
    raise ValueError(f"Unsupported URL scheme for clone: {scheme!r}")


def _fetch_https(url: str, dest: Path | None) -> bytes | None:
    with urllib.request.urlopen(url) as resp:
        if dest is None:
            return resp.read()
        with dest.open("wb") as f:
            shutil.copyfileobj(resp, f)
    return None


def _fetch_s3(url: str, dest: Path | None) -> bytes | None:
    import boto3

    rest = url.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    if not key:
        raise ValueError(f"S3 URL missing key: {url!r}")
    client = boto3.client("s3")
    if dest is None:
        obj = client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    client.download_file(bucket, key, str(dest))
    return None


def _fetch_local(path: Path, dest: Path | None) -> bytes | None:
    if not path.exists():
        raise FileNotFoundError(path)
    if dest is None:
        return path.read_bytes()
    shutil.copy2(path, dest)
    return None


def _normalize_base(url: str) -> str:
    """Ensure the base URL ends with ``/``."""
    return url if url.endswith("/") else url + "/"


def clone(
    url: str,
    *,
    project_dir: Path | None = None,
    force: bool = False,
) -> None:
    """Clone a published frozen DuckLake into a new local live catalog.

    Expects both ``<url>/fdl.toml`` and ``<url>/ducklake.duckdb`` to be
    reachable at the given base URL.

    The local live catalog is always SQLite at
    ``<project_dir>/.fdl/ducklake.sqlite``. ``[data].url`` from the published
    fdl.toml is preserved verbatim so queries read data directly from the
    publisher's location.
    """
    from fdl.ducklake import _convert_ducklake_catalog

    root = project_dir or Path.cwd()
    config_path = root / PROJECT_CONFIG
    local_sqlite = root / FDL_DIR / DUCKLAKE_SQLITE

    if config_path.exists() and not force:
        raise FileExistsError(
            f"{config_path} already exists. Use --force to overwrite."
        )
    if local_sqlite.exists() and not force:
        raise FileExistsError(
            f"{local_sqlite} already exists. Use --force to overwrite."
        )

    base = _normalize_base(url)

    # 1. Fetch fdl.toml
    toml_bytes = _fetch(base + PROJECT_CONFIG)
    if toml_bytes is None:
        raise RuntimeError(f"Failed to fetch {base}{PROJECT_CONFIG}")
    fetched = tomllib.loads(toml_bytes.decode("utf-8"))
    name = fetched.get("name")
    if not name:
        raise ValueError(f"{base}{PROJECT_CONFIG}: missing 'name'")
    fetched_data_url = fetched.get("data", {}).get("url")
    if not fetched_data_url:
        raise ValueError(f"{base}{PROJECT_CONFIG}: missing [data].url")

    console.print(f"[bold]--- clone: {name} ← {url} ---[/bold]")

    # 2. Fetch frozen duckdb to a temp location
    local_sqlite.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".duckdb")
    import os
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        console.print(f"  [dim]{DUCKLAKE_FILE}[/dim]")
        _fetch(base + DUCKLAKE_FILE, tmp_path)

        # 3. Convert duckdb → sqlite with DATA_PATH baked in from the publisher
        if local_sqlite.exists():
            local_sqlite.unlink()
        _convert_ducklake_catalog(
            tmp_path,
            local_sqlite,
            src_type="duckdb",
            dst_type="sqlite",
            data_path=fetched_data_url,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    # 4. Write local fdl.toml
    abs_sqlite = local_sqlite.resolve().as_posix()
    _write_cloned_toml(
        config_path,
        name=name,
        metadata_url=f"sqlite:///{abs_sqlite}",
        data_url=fetched_data_url,
    )

    console.print(f"[green]Cloned {name} from {url}[/green]")


def _write_cloned_toml(
    path: Path,
    *,
    name: str,
    metadata_url: str,
    data_url: str,
) -> None:
    """Write a minimal v0.11 fdl.toml for a cloned live catalog."""
    content = (
        f'name = "{name}"\n'
        f"\n"
        f"[metadata]\n"
        f'url = "{metadata_url}"\n'
        f"\n"
        f"[data]\n"
        f'url = "{data_url}"\n'
    )
    path.write_text(content)
