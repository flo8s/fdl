# dlt Integration

[dlt](https://dlthub.com/) (data load tool) is a Python library for building data pipelines.
fdl provides built-in support for using dlt as a data loading backend with DuckLake.

## Why SQLite Catalog?

DuckLake supports two catalog backends: DuckDB and SQLite.
When using dlt, the **SQLite catalog is required**.

This is because dlt manages its own DuckDB connection internally and cannot share a DuckDB-based
DuckLake catalog with fdl. SQLite, on the other hand, works as an external file that both dlt
and fdl can access independently without connection conflicts.

The SQLite catalog is only used during development. When you run `fdl push`, fdl automatically
converts it to DuckDB format for distribution.

```
Development (SQLite)          Distribution (DuckDB)
┌─────────────────────┐       ┌─────────────────────┐
│ .fdl/ducklake.sqlite│──push──▶ ducklake.duckdb    │
│ (dlt writes here)   │       │ (auto-converted)     │
└─────────────────────┘       └─────────────────────┘
```

## Setup

### 1. Install with dlt extras

```bash
uv add 'frozen-ducklake[dlt]'
```

This installs `dlt[ducklake]` as an additional dependency.

### 2. Initialize with --sqlite

```bash
fdl init my-dataset --sqlite
```

This creates `.fdl/ducklake.sqlite` instead of `.fdl/ducklake.duckdb`.

### 3. Create a dlt destination

Use `fdl.ducklake.create_destination()` in your pipeline script:

```python
import dlt
from fdl.ducklake import create_destination

destination = create_destination()

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=destination,
)

pipeline.run(your_data_source())
```

`create_destination()` automatically:

- Reads storage path from `FDL_STORAGE` env var or fdl config
- Configures S3 credentials from fdl config when using S3 storage
- Points the catalog to `.fdl/ducklake.sqlite`

### 4. Push to remote

```bash
fdl push origin
```

The SQLite catalog is automatically converted to DuckDB during push.

## Using with fdl run

`fdl run` injects `FDL_STORAGE` and other environment variables that `create_destination()` reads:

```bash
fdl run -- python pipeline.py
```

For remote storage:

```bash
fdl run origin -- python pipeline.py
```

## create_destination() Reference

```python
fdl.ducklake.create_destination(storage_path: str | None = None)
```

| Parameter | Description | Default |
|---|---|---|
| `storage_path` | Base path for data files | `FDL_STORAGE` env var, then fdl config, then `.fdl` |

Returns a dlt `ducklake` destination configured with:

- Catalog: `sqlite:///.fdl/ducklake.sqlite`
- Storage: local path or S3 with credentials from fdl config
- `override_data_path=True` for consistent file placement

When `storage_path` starts with `s3://`, S3 credentials are automatically loaded from fdl config
(`s3.endpoint`, `s3.access_key_id`, `s3.secret_access_key`).
