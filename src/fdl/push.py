"""Push: upload DuckLake catalog artifacts to S3 or local directory."""

from __future__ import annotations

import shutil
from pathlib import Path

from fdl import DUCKLAKE_FILE, META_JSON
from fdl.config import PROJECT_CONFIG
from fdl.console import console


def do_push(
    target: str,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Push catalog to a target (S3 or local).

    Raises:
        fdl.meta.PushConflictError: When the remote has been updated since
            the last pull (unless ``force=True``). Only raised for S3
            targets — local targets skip conflict detection.
    """
    from fdl import fdl_target_dir
    from fdl.config import datasource_name, find_project_dir, resolve_target
    from fdl.ducklake import convert_sqlite_to_duckdb

    dataset_dir = project_dir or find_project_dir()
    dist_dir = dataset_dir / fdl_target_dir(target)
    datasource = datasource_name(dataset_dir)

    resolved = resolve_target(target, dataset_dir)
    console.print(f"[bold]--- push: {datasource} → {resolved} ---[/bold]")
    convert_sqlite_to_duckdb(dataset_dir, target)

    if resolved.startswith("s3://"):
        from fdl.config import target_s3_config
        from fdl.s3 import create_s3_client

        s3 = target_s3_config(target, dataset_dir)
        client = create_s3_client(s3)
        push_to_s3(
            client, s3.bucket, dist_dir, datasource, dataset_dir,
            force=force, target_name=target,
        )
    else:
        push_to_local(Path(resolved), dist_dir, datasource, dataset_dir)


def push_to_local(
    output_dir: Path, dist_dir: Path, datasource: str, project_dir: Path,
) -> None:
    """Copy artifacts to a local directory.

    Local targets skip conflict detection (assumed single-user).
    """
    dest = output_dir / datasource
    dest.mkdir(parents=True, exist_ok=True)

    src = dist_dir / DUCKLAKE_FILE
    if src.exists():
        console.print(f"  [dim]{datasource}/{DUCKLAKE_FILE}[/dim]")
        shutil.copy2(src, dest / DUCKLAKE_FILE)

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
    """Upload a single file to S3 via upload_file (multipart-capable)."""
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


def _put_catalog_with_precondition(
    client,
    bucket: str,
    key: str,
    file_path: Path,
    *,
    saved_etag: str | None,
    force: bool,
) -> str:
    """PUT the catalog with If-Match / If-None-Match precondition.

    Returns the ETag of the uploaded object.

    Raises:
        fdl.meta.PushConflictError: When the server rejects the PUT with
            HTTP 412 (precondition failed), meaning another client pushed
            since the last pull.
    """
    from botocore.exceptions import ClientError

    from fdl.meta import PushConflictError

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
                "Remote catalog has been updated since the last pull. "
                "Run 'fdl pull' first, or use --force to override."
            ) from e
        raise

    return response["ETag"]


def push_to_s3(
    client, bucket: str, dist_dir: Path, datasource: str, project_dir: Path,
    *, force: bool = False, target_name: str,
) -> None:
    """Upload artifacts to S3 with ETag-based conflict detection."""
    from fdl import fdl_target_dir
    from fdl.meta import read_remote_etag, write_remote_etag

    state_path = project_dir / fdl_target_dir(target_name) / META_JSON
    saved_etag = read_remote_etag(state_path)

    # Upload auxiliary files first; the catalog goes last so that its
    # successful PUT marks the push as committed (fail-safe ordering).
    _upload_if_exists(
        client,
        bucket,
        f"{datasource}/{PROJECT_CONFIG}",
        project_dir / PROJECT_CONFIG,
        content_type="application/toml; charset=utf-8",
    )

    etag = _put_catalog_with_precondition(
        client,
        bucket,
        f"{datasource}/{DUCKLAKE_FILE}",
        dist_dir / DUCKLAKE_FILE,
        saved_etag=saved_etag,
        force=force,
    )
    write_remote_etag(state_path, etag)
