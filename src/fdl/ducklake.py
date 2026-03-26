"""DuckLake catalog management."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from fdl import FDL_DIR, DUCKLAKE_FILE, DUCKLAKE_SQLITE, ducklake_data_path


@contextmanager
def connect(
    *,
    storage: str | None = None,
) -> Generator[duckdb.DuckDBPyConnection]:
    """Connect to the DuckLake catalog and return a DuckDB connection.

    Dataset name is auto-detected from dataset.yml (or cwd directory name).
    Data file path is controlled by FDL_STORAGE env var (default: .fdl).

    Args:
        storage: Base path for data files. Reads from env var if omitted.
    """
    from fdl.config import datasource_name

    name = datasource_name()

    ducklake_path = FDL_DIR / DUCKLAKE_FILE
    if not ducklake_path.exists():
        msg = f"{ducklake_path} not found. Run 'fdl init' or 'fdl pull' first."
        raise FileNotFoundError(msg)

    if storage is None:
        from fdl.config import storage as get_storage

        storage = get_storage()
    data_path = ducklake_data_path(f"{storage}/{DUCKLAKE_FILE}")

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        if storage.startswith("s3://"):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            from fdl.config import s3_access_key_id, s3_endpoint, s3_secret_access_key

            conn.execute(f"""
                SET s3_url_style = 'path';
                SET s3_access_key_id = '{s3_access_key_id()}';
                SET s3_secret_access_key = '{s3_secret_access_key()}';
                SET s3_endpoint = '{s3_endpoint().removeprefix("https://")}';
                SET s3_region = 'auto';
            """)
        conn.execute(f"""
            ATTACH 'ducklake:{ducklake_path}' AS {name} (
                DATA_PATH '{data_path}',
                OVERRIDE_DATA_PATH true
            )
        """)
        yield conn
    finally:
        conn.close()


def create_destination(storage_path: str | None = None):
    """Create a dlt DuckLake destination.

    Args:
        storage_path: Base path for data files. Resolved from FDL_STORAGE
            env var / config if omitted. S3 paths read credentials from fdl config.
    """
    from dlt.common.storages.configuration import FilesystemConfiguration
    from dlt.destinations import ducklake
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    if storage_path is None:
        from fdl.config import storage as get_storage

        storage_path = get_storage()

    FDL_DIR.mkdir(exist_ok=True)
    ducklake_path = f"{storage_path}/{DUCKLAKE_FILE}"
    storage_url = ducklake_data_path(ducklake_path)

    if storage_path.startswith("s3://"):
        from dlt.common.configuration.specs.aws_credentials import AwsCredentials

        from fdl.config import s3_access_key_id, s3_endpoint, s3_secret_access_key

        storage = FilesystemConfiguration(
            bucket_url=storage_url,
            credentials=AwsCredentials(
                aws_access_key_id=s3_access_key_id(),
                aws_secret_access_key=s3_secret_access_key(),
                endpoint_url=s3_endpoint(),
                region_name="auto",
            ),
        )
    else:
        storage = storage_url

    return ducklake(
        credentials=DuckLakeCredentials(
            catalog=f"sqlite:///{FDL_DIR / DUCKLAKE_SQLITE}",
            storage=storage,
        ),
        override_data_path=True,
    )


def init_ducklake(dist_dir: Path, dataset_dir: Path, *, sqlite: bool = False) -> None:
    """Initialize DuckLake catalog (skip if exists)."""
    catalog_file = dist_dir / (DUCKLAKE_SQLITE if sqlite else DUCKLAKE_FILE)
    if catalog_file.exists():
        return

    from fdl.config import datasource_name, ducklake_url

    datasource = datasource_name(dataset_dir)
    data_path = ducklake_data_path(ducklake_url(datasource))
    meta_type = "sqlite" if sqlite else "duckdb"
    print(f"Creating DuckLake ({meta_type}): {datasource} (DATA_PATH: {data_path})")

    dist_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH 'ducklake:{catalog_file}' AS {datasource} (
            DATA_PATH '{data_path}',
            META_TYPE '{meta_type}'
        )
    """)
    conn.close()


def convert_sqlite_to_duckdb(dataset_dir: Path) -> None:
    """Convert SQLite catalog to DuckDB format, replacing ducklake.duckdb."""
    dist_dir = dataset_dir / FDL_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    if not sqlite_file.exists():
        return

    from fdl.config import datasource_name, ducklake_url

    datasource = datasource_name(dataset_dir)
    data_path = ducklake_data_path(ducklake_url(datasource))

    print("Converting DuckLake: SQLite -> DuckDB")
    SRC = "src"
    DST = "dst"
    tmp_file = duckdb_file.with_suffix(".duckdb.tmp")
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")

        conn.execute(f"""
            ATTACH 'ducklake:{tmp_file}' AS {DST} (DATA_PATH '{data_path}')
        """)

        conn.execute(f"DETACH {DST}")
        conn.execute(f"ATTACH '{sqlite_file}' AS {SRC} (TYPE sqlite)")
        conn.execute(f"ATTACH '{tmp_file}' AS {DST}")

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog='{SRC}'"
        ).fetchall()
        for (table_name,) in tables:
            conn.execute(f"DELETE FROM {DST}.main.{table_name}")
            conn.execute(
                f"INSERT INTO {DST}.main.{table_name} "
                f"SELECT * FROM {SRC}.main.{table_name}"
            )

        conn.execute(f"CHECKPOINT {DST}")
        conn.close()

        if duckdb_file.exists():
            duckdb_file.unlink()
        tmp_file.rename(duckdb_file)
    finally:
        for f in [tmp_file, tmp_file.with_suffix(".duckdb.tmp.wal")]:
            if f.exists():
                f.unlink()
