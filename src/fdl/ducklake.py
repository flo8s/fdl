"""DuckLake catalog management."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from fdl import DIST_DIR, DUCKLAKE_FILE, DUCKLAKE_SQLITE, ducklake_data_path
from fdl.config_schema import load_dataset_config


@contextmanager
def connect(
    *,
    storage: str | None = None,
) -> Generator[duckdb.DuckDBPyConnection]:
    """DuckLake カタログに接続し DuckDB 接続を返す。

    データセット名は dataset.yml (または cwd のディレクトリ名) から自動検出する。
    データファイルのパスは DUCKLAKE_STORAGE 環境変数で制御 (デフォルト "dist")。

    Args:
        storage: データファイルのベースパス。省略時は環境変数から読み取り。
    """
    config = load_dataset_config(Path.cwd())
    name = config.name

    ducklake_path = DIST_DIR / DUCKLAKE_FILE
    if not ducklake_path.exists():
        msg = f"{ducklake_path} not found. Run 'fdl init' or 'fdl pull' first."
        raise FileNotFoundError(msg)

    if storage is None:
        storage = os.environ.get("DUCKLAKE_STORAGE", str(DIST_DIR))
    data_path = ducklake_data_path(f"{storage}/{DUCKLAKE_FILE}")

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        if storage.startswith("s3://"):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"""
                SET s3_url_style = 'path';
                SET s3_access_key_id = '{os.environ["S3_ACCESS_KEY_ID"]}';
                SET s3_secret_access_key = '{os.environ["S3_SECRET_ACCESS_KEY"]}';
                SET s3_endpoint = '{os.environ["S3_ENDPOINT"]}';
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


def create_destination(storage_path: str = str(DIST_DIR)):
    """dlt DuckLake destination を作成する。

    storage_path でデータの書き込み先ベースパスを指定する。
    省略時はローカル dist/ に書き込む。
    S3 パスの場合は環境変数から R2 credentials を読み取る。
    """
    from dlt.common.storages.configuration import FilesystemConfiguration
    from dlt.destinations import ducklake
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    DIST_DIR.mkdir(exist_ok=True)
    ducklake_path = f"{storage_path}/{DUCKLAKE_FILE}"
    storage_url = ducklake_data_path(ducklake_path)

    if storage_path.startswith("s3://"):
        from dlt.common.configuration.specs.aws_credentials import AwsCredentials

        storage = FilesystemConfiguration(
            bucket_url=storage_url,
            credentials=AwsCredentials(
                aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                endpoint_url=f"https://{os.environ['S3_ENDPOINT']}",
                region_name="auto",
            ),
        )
    else:
        storage = storage_url

    return ducklake(
        credentials=DuckLakeCredentials(
            catalog=f"sqlite:///{DIST_DIR / DUCKLAKE_SQLITE}",
            storage=storage,
        ),
        override_data_path=True,
    )


def init_ducklake(dist_dir: Path, dataset_dir: Path, *, sqlite: bool = False) -> None:
    """Initialize DuckLake catalog (skip if exists)."""
    catalog_file = dist_dir / (DUCKLAKE_SQLITE if sqlite else DUCKLAKE_FILE)
    if catalog_file.exists():
        return

    config = load_dataset_config(dataset_dir)
    datasource = config.name
    data_path = ducklake_data_path(config.ducklake_url)
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
    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    if not sqlite_file.exists():
        return

    config = load_dataset_config(dataset_dir)
    data_path = ducklake_data_path(config.ducklake_url)

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
