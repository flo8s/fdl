# fdl — Frozen DuckLake CLI

[![PyPI](https://img.shields.io/pypi/v/frozen-ducklake)](https://pypi.org/project/frozen-ducklake/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue)](https://pypi.org/project/frozen-ducklake/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Manage [Frozen DuckLake](https://ducklake.select/2025/10/24/frozen-ducklake/) catalogs — keep a live DuckLake catalog (SQLite or PostgreSQL) and publish read-only DuckDB snapshots to object storage that anyone can query with a single DuckDB `ATTACH`.

## Quick Start

```bash
pip install frozen-ducklake

mkdir my_dataset && cd my_dataset
fdl init my_dataset --publish-url ./dist
```

Add data, then publish a frozen snapshot:

```bash
fdl sql "CREATE TABLE cities (name VARCHAR, country VARCHAR, pop INTEGER)"
fdl sql "INSERT INTO cities VALUES ('Tokyo', 'Japan', 14000000), ('Shanghai', 'China', 24900000)"

fdl publish
fdl serve
```

Query from DuckDB:

```bash
duckdb -c "ATTACH 'ducklake:http://localhost:4001/ducklake.duckdb' AS my_dataset;
           SELECT * FROM my_dataset.main.cities ORDER BY pop DESC;"
```

Refresh a local SQLite live catalog from the latest frozen snapshot:

```bash
fdl pull                     # rebuilds .fdl/ducklake.sqlite from [publishes.*]
fdl sql "SELECT COUNT(*) FROM cities"
```

## Concepts

Three orthogonal sections in `fdl.toml`:

- `[metadata]` — live catalog DB (SQLite or PostgreSQL), where writes happen
- `[data]` — where Parquet files live (local path or `s3://`)
- `[publishes.<name>]` — zero or more frozen-snapshot destinations

## Features

- SQLite or PostgreSQL live catalog — PostgreSQL supports true concurrent writes
- `fdl pull` / `fdl publish` — explicit frozen-snapshot workflow (SQLite-only for pull)
- `fdl run` — execute a pipeline then publish atomically on success
- `fdl sql` / `fdl duckdb` — interactive querying of the live catalog
- `fdl serve` — built-in HTTP server with CORS and Range support
- Python API mirrors the CLI for Dagster / Airflow / notebook use
- Works with S3-compatible storage (AWS S3, Cloudflare R2, GCS, etc.) and local directories

## Install

```bash
pip install frozen-ducklake   # pip
uv tool install frozen-ducklake   # uv (recommended)
pipx install frozen-ducklake   # pipx
```

## Documentation

[fdl.flo8s.com](https://fdl.flo8s.com/)

## License

MIT
