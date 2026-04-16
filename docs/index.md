---
title: Home
---

# fdl — Frozen DuckLake CLI

fdl automates the lifecycle of [Frozen DuckLake](https://ducklake.select/2025/10/24/frozen-ducklake/) catalogs — from initialization to deployment.

## Installation

```bash
pip install frozen-ducklake
```

See [Installation](getting-started/installation.md) for other methods (uv, pipx).

## Features

- Publish data to S3-compatible storage or local directories with a single command
- Run data pipelines with automatic storage and credential resolution
- Serve datasets over HTTP for instant DuckDB access
- Works with dbt and dlt out of the box
- CLI and Python API share the same operations, so pipelines (e.g. Dagster) can drive fdl in-process

## Learn More

- [Quick Start](getting-started/quickstart.md) — Create and serve your first dataset in under 5 minutes
- [Working with Data](guide/working-with-data.md) — How to read and write data in your catalog
- [CLI Reference](reference/cli.md) — All available commands
- [Python API Reference](reference/python-api.md) — Call fdl from Python
