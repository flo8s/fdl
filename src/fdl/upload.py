"""Upload helpers for fdl publish (S3 with ETag precondition, or local copy)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from fdl.console import console
from fdl.meta import PushConflictError

if TYPE_CHECKING:
    from fdl.s3 import S3Config


def upload_publish(
    publish_url: str,
    *,
    fdl_toml: Path,
    duckdb_file: Path,
    saved_etag: str | None,
    force: bool,
    s3_config: "S3Config | None",
) -> str | None:
    """Upload a frozen catalog + fdl.toml to a publish destination.

    For S3, the catalog PUT uses an If-Match / If-None-Match precondition
    to detect concurrent publishers; conflicts raise ``PushConflictError``.
    Returns the server-reported ETag for S3 uploads, ``None`` for local.
    """
    if publish_url.startswith("s3://"):
        if s3_config is None:
            raise ValueError("S3 credentials required for s3:// publish URL")
        return _upload_s3(
            publish_url,
            fdl_toml=fdl_toml,
            duckdb_file=duckdb_file,
            saved_etag=saved_etag,
            force=force,
            s3_config=s3_config,
        )
    if publish_url.startswith(("http://", "https://")):
        raise ValueError(
            "Cannot publish to an HTTP URL; use s3:// or a local path"
        )
    local_base = publish_url.removeprefix("file://")
    _upload_local(Path(local_base), fdl_toml=fdl_toml, duckdb_file=duckdb_file)
    return None


def _upload_local(
    dest_dir: Path,
    *,
    fdl_toml: Path,
    duckdb_file: Path,
) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"  [dim]{fdl_toml.name}[/dim]")
    shutil.copy2(fdl_toml, dest_dir / fdl_toml.name)
    console.print(f"  [dim]{duckdb_file.name}[/dim]")
    shutil.copy2(duckdb_file, dest_dir / duckdb_file.name)


def _upload_s3(
    publish_url: str,
    *,
    fdl_toml: Path,
    duckdb_file: Path,
    saved_etag: str | None,
    force: bool,
    s3_config: "S3Config",
) -> str:
    from fdl.s3 import create_s3_client

    rest = publish_url.removeprefix("s3://")
    bucket, _, prefix = rest.partition("/")
    prefix = prefix.rstrip("/")
    client = create_s3_client(s3_config)

    # Upload fdl.toml first (sibling file is cosmetic) then the catalog;
    # the catalog PUT is the commit point and carries the precondition.
    toml_key = f"{prefix}/{fdl_toml.name}" if prefix else fdl_toml.name
    console.print(f"  [dim]{toml_key}[/dim]")
    with fdl_toml.open("rb") as f:
        client.put_object(
            Bucket=bucket,
            Key=toml_key,
            Body=f,
            ContentType="application/toml; charset=utf-8",
        )

    catalog_key = f"{prefix}/{duckdb_file.name}" if prefix else duckdb_file.name
    return _put_with_precondition(
        client,
        bucket=bucket,
        key=catalog_key,
        file_path=duckdb_file,
        saved_etag=saved_etag,
        force=force,
    )


def _put_with_precondition(
    client,
    *,
    bucket: str,
    key: str,
    file_path: Path,
    saved_etag: str | None,
    force: bool,
) -> str:
    """PUT with If-Match / If-None-Match, raising on 412."""
    from botocore.exceptions import ClientError

    console.print(f"  [dim]{key}[/dim]")
    kwargs: dict = {
        "Bucket": bucket,
        "Key": key,
        "CacheControl": "no-cache",
    }
    if not force:
        if saved_etag is None:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = saved_etag
    try:
        with file_path.open("rb") as f:
            kwargs["Body"] = f
            response = client.put_object(**kwargs)
    except ClientError as e:
        code = e.response["Error"].get("Code", "")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code == "PreconditionFailed" or status == 412:
            raise PushConflictError(
                "Remote has been updated since the last publish. "
                "Run 'fdl pull' first, or pass --force to override."
            ) from e
        raise
    return response["ETag"]
