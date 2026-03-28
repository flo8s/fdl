# dlt

[dlt](https://dlthub.com/) (data load tool) is a Python library for building data pipelines.
fdl supports using dlt as a data loading backend with DuckLake.

## Why SQLite Catalog?

DuckLake supports two catalog backends: DuckDB and SQLite.
When using dlt, the SQLite catalog is required.

dlt manages its own DuckDB connection internally and cannot share a DuckDB-based catalog. SQLite works as an external file that both dlt and DuckDB can access independently.

See [dlt DuckLake catalog configuration](https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake#configure-catalog) for details.

The SQLite catalog is only used during development. `fdl push` automatically converts it to DuckDB format for distribution.

## Setup

### 1. Initialize with --sqlite

```bash
fdl init my_dataset --sqlite
```

This creates `.fdl/ducklake.sqlite` instead of `.fdl/ducklake.duckdb`.

### 2. Configure dlt destination

`fdl run` injects `FDL_CATALOG`, `FDL_DATA_PATH`, and S3 credentials as environment variables. Use `FDL_CATALOG` to configure the dlt destination:

```python
import os
import dlt
from dlt.destinations import ducklake

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=ducklake(
        credentials=f"sqlite:///{os.environ['FDL_CATALOG']}",
        bucket_url=os.environ["FDL_DATA_PATH"],
        override_data_path=True,
    ),
)

pipeline.run(your_data_source())
```

See [dlt DuckLake destination docs](https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake) for full configuration options including S3 storage and credentials via environment variables.

### 3. Run with fdl

```bash
fdl run default -- python pipeline.py
```

### 4. Push to target

```bash
fdl push default
```

The SQLite catalog is automatically converted to DuckDB during push.
