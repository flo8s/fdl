# Quick Start

Create your first Frozen DuckLake in under 5 minutes — no S3, no dbt, no external services required.

## 1. Install

```bash
pip install frozen-ducklake
```

For other methods (uv, pipx), see [Installation](installation.md).

## 2. Create a project

```bash
mkdir my_dataset && cd my_dataset
fdl init my_dataset --publish-url ./dist
```

This creates:

- `fdl.toml` — project config with `[metadata]`, `[data]`, and `[publishes.default]` sections
- `.fdl/ducklake.sqlite` — the live SQLite catalog
- `.fdl/data/` — Parquet data file directory

Defaults: live catalog in `./.fdl/ducklake.sqlite`, data in `./.fdl/data/`, publish destination in `./dist/`.

## 3. Add data

```bash
fdl sql "CREATE TABLE world_cities (name VARCHAR, country VARCHAR, population INTEGER)"
fdl sql "INSERT INTO world_cities VALUES
    ('Tokyo', 'Japan', 14000000),
    ('Delhi', 'India', 11000000),
    ('Shanghai', 'China', 24900000),
    ('São Paulo', 'Brazil', 12300000),
    ('Mumbai', 'India', 12500000),
    ('Beijing', 'China', 21500000)"
```

`fdl sql` writes directly to the live catalog. Row data goes to `./.fdl/data/` as Parquet.

Verify:

```bash
fdl sql "SELECT * FROM world_cities ORDER BY population DESC LIMIT 5"
```

## 4. Publish and serve

Publish a frozen snapshot, then serve it:

```bash
fdl publish
fdl serve
```

`fdl publish` converts the live SQLite catalog to a DuckDB file and uploads it (alongside `fdl.toml`) to `./dist/`. `fdl serve` starts an HTTP server on port 4001 with CORS and Range support.

## 5. Query from DuckDB

In another terminal:

```bash
duckdb -c "
  ATTACH 'ducklake:http://localhost:4001/ducklake.duckdb' AS my_dataset;
  SELECT count(*) FROM my_dataset.main.world_cities;
"
```

## Optional: Publish to S3-compatible storage

Initialize with an S3 destination:

```bash
fdl init my_dataset \
  --data-url 's3://${FDL_S3_BUCKET}/my_dataset' \
  --publish-url 's3://${FDL_PUBLISH_BUCKET}/my_dataset'
```

Then add credentials in `fdl.toml` using `${VAR}` references:

```toml
[data]
url = "s3://${FDL_S3_BUCKET}/my_dataset"
s3_endpoint = "https://${CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
s3_access_key_id = "${S3_ACCESS_KEY_ID}"
s3_secret_access_key = "${S3_SECRET_ACCESS_KEY}"

[publishes.public]
url = "s3://${FDL_PUBLISH_BUCKET}/my_dataset"
public_url = "https://data.example.com"
```

Once deployed, anyone can query directly from the public URL:

```bash
duckdb -c "ATTACH 'ducklake:https://data.example.com/ducklake.duckdb' AS my_dataset"
```

## From Python

The same operations are available as a Python API, useful for pipelines and orchestrators (e.g. Dagster):

```python
import fdl

with fdl.connect() as conn:
    conn.execute(
        "CREATE TABLE world_cities (name VARCHAR, country VARCHAR, population INTEGER)"
    )
    conn.execute("INSERT INTO world_cities VALUES ('Tokyo', 'Japan', 14000000)")

fdl.publish()
```

See the [Python API Reference](../reference/python-api.md) for all entry points.

## Next steps

- [Working with Data](../guide/working-with-data.md) — live/frozen workflow, env vars, fdl run
- [Configuration](../guide/configuration.md) — [metadata]/[data]/[publishes] schema, SQLite vs PostgreSQL
- [CLI Reference](../reference/cli.md) — all available commands
- [Python API Reference](../reference/python-api.md) — call fdl from Python
