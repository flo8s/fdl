# dlt

[dlt](https://dlthub.com/) (data load tool) is a Python library for building data pipelines.
fdl supports using dlt as a data loading backend with DuckLake.

## Catalog compatibility

As of v0.9, fdl always stores the local catalog as SQLite (`.fdl/{target}/ducklake.sqlite`). dlt manages its own DuckDB connection internally and cannot share a DuckDB-based catalog, so SQLite is what lets dlt and fdl (plus `fdl sql`) touch the same catalog from separate processes.

The SQLite catalog is only used during development. `fdl push` converts it to DuckDB format for distribution. See [dlt DuckLake catalog configuration](https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake#configure-catalog) for the dlt side of the setup.

## Setup

### 1. Initialize the project

```bash
fdl init my_dataset
```

This creates `.fdl/{target}/ducklake.sqlite`.

### 2. Configure dlt destination

`fdl run` injects `FDL_CATALOG_URL`, `FDL_DATA_URL`, and S3 credentials as environment variables. Pass them straight into the dlt destination:

```python
import os
import dlt
from dlt.destinations import ducklake

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=ducklake(
        credentials=os.environ["FDL_CATALOG_URL"],
        bucket_url=os.environ["FDL_DATA_URL"],
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

The SQLite catalog is converted to DuckDB during push.
