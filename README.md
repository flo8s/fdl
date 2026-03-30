# fdl — Frozen DuckLake CLI

[![PyPI](https://img.shields.io/pypi/v/frozen-ducklake)](https://pypi.org/project/frozen-ducklake/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue)](https://pypi.org/project/frozen-ducklake/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Manage [Frozen DuckLake](https://ducklake.select/2025/10/24/frozen-ducklake/) catalogs — place a DuckLake catalog + Parquet files on object storage and anyone can query with a single DuckDB `ATTACH`. No database server required.

## Quick Start

```bash
pip install frozen-ducklake

mkdir my_dataset && cd my_dataset
fdl init my_dataset
```

Add data, push, and serve:

```bash
fdl sql default "CREATE TABLE cities (name VARCHAR, country VARCHAR, pop INTEGER)"
fdl sql default "INSERT INTO cities VALUES ('Tokyo', 'Japan', 14000000), ('Shanghai', 'China', 24900000)"

fdl push default
fdl serve default
```

Query from DuckDB:

```bash
duckdb -c "ATTACH 'ducklake:http://localhost:4001/my_dataset/ducklake.duckdb' AS my_dataset;
           SELECT * FROM my_dataset.main.cities ORDER BY pop DESC;"
```

## Features

- init / push / pull — Git-like catalog management with conflict detection
- sql — Query and modify data directly from the command line
- run — Execute any command (dbt, dlt, Python) with auto-injected storage credentials
- serve — Built-in HTTP server with CORS and Range request support
- Works with S3-compatible storage (AWS S3, Cloudflare R2, GCS, etc) and local directories

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
