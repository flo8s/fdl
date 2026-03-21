"""fdl CLI entry point."""

from pathlib import Path

import typer
from typer.main import Typer

from fdl import DIST_DIR

app = typer.Typer()


@app.callback()
def callback() -> None:
    """fdl: DuckLake catalog management CLI"""


@app.command()
def init(
    sqlite: bool = typer.Option(
        False, help="Initialize as SQLite (for dlt compatibility)"
    ),
) -> None:
    """Initialize DuckLake catalog"""
    from fdl.ducklake import init_ducklake

    dataset_dir = Path.cwd()
    init_ducklake(dataset_dir / DIST_DIR, dataset_dir, sqlite=sqlite)


@app.command()
def pull(
    source: str = typer.Argument(None, help="Source (local path or s3://bucket)"),
    sqlite: bool = typer.Option(
        False, help="Initialize as SQLite (for dlt compatibility)"
    ),
) -> None:
    """Pull DuckLake catalog from source (init if not found)"""
    from fdl.config_schema import load_dataset_config
    from fdl.ducklake import init_ducklake

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / DIST_DIR
    config = load_dataset_config(dataset_dir)
    datasource = config.name

    source = source or config.s3_url
    if not source:
        raise typer.BadParameter("source argument or 's3_url' in dataset.yml required")

    print(f"--- pull: {datasource} ---")

    if source.startswith("s3://"):
        from fdl.pull import fetch_from_s3
        from fdl.s3 import create_s3_client

        bucket = source.removeprefix("s3://")
        client = create_s3_client()
        fetched = fetch_from_s3(client, bucket, dist_dir, datasource)
    else:
        from fdl.pull import pull_from_local

        fetched = pull_from_local(Path(source), dist_dir, datasource)

    if not fetched:
        print("Catalog not found, initializing locally")
        init_ducklake(dist_dir, dataset_dir, sqlite=sqlite)


@app.command()
def push(
    dest: str = typer.Argument(None, help="Destination (local path or s3://bucket)"),
) -> None:
    """Push build artifacts"""
    from fdl.config_schema import load_dataset_config
    from fdl.ducklake import convert_sqlite_to_duckdb

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / DIST_DIR
    config = load_dataset_config(dataset_dir)
    datasource = config.name

    dest = dest or config.s3_url
    if not dest:
        raise typer.BadParameter("dest argument or 's3_url' in dataset.yml required")

    print(f"--- push: {datasource} ---")
    convert_sqlite_to_duckdb(dataset_dir)

    if dest.startswith("s3://"):
        from fdl.push import push_to_s3
        from fdl.s3 import create_s3_client

        bucket = dest.removeprefix("s3://")
        client = create_s3_client()
        push_to_s3(client, bucket, dist_dir, datasource)
    else:
        from fdl.push import push_to_local

        push_to_local(Path(dest), dist_dir, datasource)


@app.command()
def metadata(
    target_dir: str = typer.Argument("target", help="dbt target directory path"),
) -> None:
    """Generate metadata.json from dbt artifacts"""
    from fdl.metadata import _copy_docs_to_dist, generate_metadata

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / DIST_DIR
    target_path = Path(target_dir)
    generate_metadata(dataset_dir, dist_dir, target_path)
    _copy_docs_to_dist(target_path, dist_dir)


main: Typer = app

if __name__ == "__main__":
    main()
