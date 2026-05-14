"""Publish: convert a live catalog to a frozen DuckDB and upload it."""

from __future__ import annotations

from pathlib import Path

from fdl import DUCKLAKE_FILE, FDL_DIR, META_JSON
from fdl.config import PROJECT_CONFIG
from fdl.console import console


def publish(
    name: str | None = None,
    *,
    project_dir: Path | None = None,
    force: bool = False,
    keep_intermediate: bool = False,
) -> None:
    """Convert the live catalog to a frozen DuckDB and upload it.

    Args:
        name: Publish name from ``[publishes.<name>]``. Defaults to the sole
            entry when only one is defined.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.
        force: Override ETag precondition checks for S3 destinations.
        keep_intermediate: Retain ``.fdl/publishes/<name>/ducklake.duckdb``
            after upload (useful for debugging).
    """
    from fdl.config import (
        data_url,
        datasource_name,
        find_project_dir,
        metadata_schema,
        metadata_spec,
        publish_public_url,
        publish_s3_config,
        publish_url,
        resolve_publish_name,
    )
    from fdl.ducklake import _convert_ducklake_catalog
    from fdl.meta import read_remote_etag, write_remote_etag
    from fdl.upload import upload_publish

    root = project_dir or find_project_dir()
    name = resolve_publish_name(name, root)

    pub_url = publish_url(name, root)
    public_url = publish_public_url(name, root)
    live_data_url = data_url(root)
    datasource = datasource_name(root)

    console.print(f"[bold]--- publish: {datasource} → {pub_url} ---[/bold]")

    spec = metadata_spec(root)
    schema = metadata_schema(root)

    # The DATA_PATH baked into the frozen catalog: prefer public_url when
    # the publisher serves data separately; else inherit the live data URL.
    frozen_data_path = public_url or live_data_url

    # Intermediate directory scoped to this publish destination.
    work_dir = root / FDL_DIR / "publishes" / name
    work_dir.mkdir(parents=True, exist_ok=True)
    tmp_duckdb = work_dir / DUCKLAKE_FILE
    if tmp_duckdb.exists():
        tmp_duckdb.unlink()

    if spec.scheme == "sqlite":
        if spec.path is None:
            raise ValueError("sqlite metadata spec missing path")
        _convert_ducklake_catalog(
            Path(spec.path),
            tmp_duckdb,
            src_type="sqlite",
            dst_type="duckdb",
            data_path=frozen_data_path,
        )
    elif spec.scheme == "postgres":
        # Postgres live catalog support lands alongside the Phase 3 convert
        # extension; sqlite is enough to validate the publish/clone loop.
        raise NotImplementedError(
            "publish from postgres metadata is not yet implemented"
        )
    else:
        raise ValueError(f"Unsupported metadata scheme: {spec.scheme}")
    _ = schema  # schema override is only relevant when attaching postgres live

    state_path = work_dir / META_JSON
    saved_etag = read_remote_etag(state_path)
    new_etag = upload_publish(
        pub_url,
        fdl_toml=root / PROJECT_CONFIG,
        duckdb_file=tmp_duckdb,
        saved_etag=saved_etag,
        force=force,
        s3_config=publish_s3_config(name, root),
    )
    if new_etag is not None:
        write_remote_etag(state_path, new_etag)

    if not keep_intermediate:
        tmp_duckdb.unlink(missing_ok=True)

    console.print(f"[green]Published {datasource} to {pub_url}[/green]")
