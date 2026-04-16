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
fdl init my_dataset
```

You'll be prompted for target configuration. Press Enter to accept all defaults:

```
Target name [default]:
Public URL [http://localhost:4001]:
Target URL [~/.local/share/fdl]:
```

- Target name defaults to `default` — the default deploy destination
- Public URL defaults to `http://localhost:4001` — matches the port `fdl serve` uses, so your catalog works immediately for local HTTP access
- Target URL defaults to `~/.local/share/fdl` — a central location where pushed datasets accumulate across projects

For S3-compatible storage deployment, set `--public-url` to your public endpoint at init time. Public URL is baked into the catalog and cannot be changed later — see [Known Issues](../resources/known-issues.md#data_path-is-fixed-at-init-time) for details.

This creates `fdl.toml` (project config with target settings) and a `.fdl/{target}/` directory containing the DuckLake catalog.

## 3. Add data

```bash
fdl sql default "CREATE TABLE world_cities (name VARCHAR, country VARCHAR, population INTEGER)"
fdl sql default "INSERT INTO world_cities VALUES
    ('Tokyo', 'Japan', 14000000),
    ('Delhi', 'India', 11000000),
    ('Shanghai', 'China', 24900000),
    ('São Paulo', 'Brazil', 12300000),
    ('Mexico City', 'Mexico', 9200000),
    ('Cairo', 'Egypt', 10200000),
    ('Mumbai', 'India', 12500000),
    ('Beijing', 'China', 21500000),
    ('Dhaka', 'Bangladesh', 8900000),
    ('Osaka', 'Japan', 2800000)"
```

`fdl sql` writes data directly to the target (`~/.local/share/fdl/my_dataset/` by default). The table definition is stored in the catalog (`.fdl/{target}/ducklake.duckdb`) and the row data is written as Parquet files to the target storage.

Verify the data:

```bash
fdl sql default "SELECT * FROM world_cities ORDER BY population DESC LIMIT 5"
```

```
name     | country | population
---------+---------+-----------
Shanghai | China   | 24900000
Beijing  | China   | 21500000
Tokyo    | Japan   | 14000000
Mumbai   | India   | 12500000
São Paulo | Brazil  | 12300000
```

## 4. Push catalog and serve

Push the catalog to the target, then serve:

```bash
fdl push default
fdl serve default
```

`fdl push` copies the catalog to the target directory. `fdl serve` starts an HTTP server (port 4001) serving the target with CORS and Range request support.

In another terminal:

```bash
duckdb -c "
  ATTACH 'ducklake:http://localhost:4001/my_dataset/ducklake.duckdb' AS my_dataset;
  SELECT * FROM my_dataset.main.world_cities ORDER BY population DESC;
"
```

DuckDB fetches the catalog metadata over HTTP, then reads Parquet data files on demand.

## Optional: Deploy to S3-compatible storage

Initialize with your S3 public URL:

```bash
fdl init my_dataset --public-url https://your-public-url.com --target-url 's3://${FDL_S3_BUCKET}'
```

Then add S3 credentials to `fdl.toml` using `${VAR}` references:

```toml
[targets.default]
url = "s3://${FDL_S3_BUCKET}"
public_url = "https://your-public-url.com"
s3_endpoint = "https://${CF_ACCOUNT_ID}.r2.cloudflarestorage.com"
s3_access_key_id = "${S3_ACCESS_KEY_ID}"
s3_secret_access_key = "${S3_SECRET_ACCESS_KEY}"
```

Set environment variables (e.g. via `.envrc`), then add data and push:

```bash
fdl sql default "CREATE TABLE world_cities (name VARCHAR, country VARCHAR, population INTEGER)"
fdl sql default "INSERT INTO world_cities VALUES ('Tokyo', 'Japan', 14000000)"
fdl push default
```

Once deployed, anyone can query directly from the public URL:

```bash
duckdb -c "ATTACH 'ducklake:https://your-public-url.com/my_dataset/ducklake.duckdb' AS my_dataset"
```

## From Python

The same operations are available as a Python API, useful for pipelines and
orchestrators (e.g. Dagster):

```python
import fdl

with fdl.connect("default") as conn:
    conn.execute(
        "CREATE TABLE world_cities (name VARCHAR, country VARCHAR, population INTEGER)"
    )
    conn.execute("INSERT INTO world_cities VALUES ('Tokyo', 'Japan', 14000000)")

fdl.push("default")
```

See the [Python API Reference](../reference/python-api.md) for all entry
points.

## Next steps

- [Working with Data](../guide/working-with-data.md) — How to read and write data in your catalog
- [Configuration](../guide/configuration.md) — Targets, credentials, config layers
- [CLI Reference](../reference/cli.md) — All available commands
- [Python API Reference](../reference/python-api.md) — Call fdl from Python
- [Dagster integration](../integrations/dagster.md) — Drive fdl from Dagster assets
