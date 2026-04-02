# Working with Data

## fdl run

`fdl run` executes a command with fdl environment variables injected. This lets pipeline tools (dbt, Python scripts, etc.) connect to the DuckLake catalog without manually setting up paths and credentials.

```bash
fdl run TARGET -- COMMAND [ARGS...]
```

```bash
fdl run default -- dbt run
fdl run default -- python pipeline.py
```

`fdl run` resolves the target URL, configures storage paths and S3 credentials, then injects them as environment variables into the subprocess. Your pipeline code doesn't need to know about fdl's config system — it just reads standard environment variables.

### Injected variables

| Variable | Description |
|---|---|
| `FDL_STORAGE` | Target storage path (`{target_url}/{datasource}`) |
| `FDL_DATA_PATH` | Parquet data files directory (`{FDL_STORAGE}/ducklake.duckdb.files/`) |
| `FDL_CATALOG` | Local catalog file path (auto-detects `.duckdb` or `.sqlite`) |
| `FDL_S3_ENDPOINT` | S3 endpoint URL (S3 targets only) |
| `FDL_S3_ACCESS_KEY_ID` | S3 access key (S3 targets only) |
| `FDL_S3_SECRET_ACCESS_KEY` | S3 secret key (S3 targets only) |
| `FDL_S3_ENDPOINT_HOST` | S3 endpoint without scheme (auto-derived) |

Local target example:

```bash
FDL_STORAGE=~/.local/share/fdl/my_dataset
FDL_DATA_PATH=~/.local/share/fdl/my_dataset/ducklake.duckdb.files/
FDL_CATALOG=.fdl/{target}/ducklake.duckdb
```

S3 target example:

```bash
FDL_STORAGE=s3://my-bucket/my_dataset
FDL_DATA_PATH=s3://my-bucket/my_dataset/ducklake.duckdb.files/
FDL_CATALOG=.fdl/{target}/ducklake.duckdb
FDL_S3_ENDPOINT=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
FDL_S3_ACCESS_KEY_ID=...
FDL_S3_SECRET_ACCESS_KEY=...
FDL_S3_ENDPOINT_HOST=YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
```

### Override behavior

Existing environment variables are never overwritten. If `FDL_S3_ENDPOINT` is already set in your shell, `fdl run` will not replace it.

This means in CI/CD, you can set variables explicitly and `fdl run` will respect them:

```yaml
env:
  FDL_S3_ENDPOINT: https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
steps:
  - run: fdl run default -- python pipeline.py
  # FDL_S3_ENDPOINT keeps the value set above
```

## fdl sql

Execute SQL directly against the DuckLake catalog:

```bash
fdl sql default "CREATE TABLE cities (name VARCHAR, population INTEGER)"
fdl sql default "INSERT INTO cities VALUES ('Tokyo', 14000000)"
fdl sql default "SELECT * FROM cities"
```

`fdl sql` opens a DuckDB connection, ATTACHes the catalog, executes the query, and closes. Data is written directly to the target storage.

Each call is a separate connection — no transactions across multiple `fdl sql` invocations.

## Verify with fdl serve

After writing data, verify it's accessible over HTTP:

```bash
fdl serve default
```

In another terminal:

```bash
duckdb -c "
  ATTACH 'ducklake:http://localhost:4001/my_dataset/ducklake.duckdb' AS my_dataset;
  SELECT count(*) FROM my_dataset.main.cities;
"
```

`fdl serve` starts an HTTP server with CORS and Range request support — everything DuckDB needs to ATTACH a remote DuckLake catalog.
