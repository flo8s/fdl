# Working with Data

## fdl sync

`fdl sync` runs your pipeline and pushes the result to the target in one step.

```bash
fdl sync TARGET
fdl sync TARGET -- COMMAND [ARGS...]
```

```bash
fdl sync default                         # uses command from fdl.toml
fdl sync default -- python main.py       # explicit command
```

Define the default command in `fdl.toml`:

```toml
command = "python main.py"
```

Per-target overrides are also supported (via `targets.<name>.command`).

Processing:

1. Auto-pull if local catalog is missing or stale
2. Run command with FDL_* environment variables injected
3. Push catalog to target (only on success)

If the command exits with a non-zero code, push is skipped and the exit code is propagated.

If the target has no catalog (locally or remotely), step 1 raises an error pointing at `fdl init` / `fdl pull TARGET` instead of silently creating an empty one.

Options:

| Option | Description |
|---|---|
| `--force`, `-f` | Override conflict detection on push |

### Injected variables

The pipeline command receives these environment variables:

| Variable | Description | Presence |
|---|---|---|
| `FDL_CATALOG_URL` | DuckLake catalog connection URL (`sqlite:///<abs>`) | always |
| `FDL_CATALOG_PATH` | Local catalog file absolute path | always |
| `FDL_DATA_URL` | Parquet data files directory (URL for S3, absolute path for local) | always |
| `FDL_DATA_BUCKET` | S3 bucket (parsed from target URL) | S3 targets only |
| `FDL_DATA_PREFIX` | S3 object prefix (ends with `ducklake.duckdb.files/`) | S3 targets only |
| `FDL_S3_ENDPOINT` | S3 endpoint URL | S3 targets only |
| `FDL_S3_ENDPOINT_HOST` | S3 endpoint without scheme (auto-derived) | S3 targets only |
| `FDL_S3_ACCESS_KEY_ID` | S3 access key | S3 targets only |
| `FDL_S3_SECRET_ACCESS_KEY` | S3 secret key | S3 targets only |

The catalog is always a local SQLite file (pulled from the target if remote).
`FDL_CATALOG_URL` is what DuckLake / dlt / dbt-ducklake expect as a connection string, while `FDL_CATALOG_PATH` is handy for direct file operations (`sqlite3`, backups).

Local target example:

```bash
FDL_CATALOG_URL=sqlite:////home/you/.fdl/default/ducklake.sqlite
FDL_CATALOG_PATH=/home/you/.fdl/default/ducklake.sqlite
FDL_DATA_URL=/home/you/.fdl/default/ducklake.duckdb.files/
```

S3 target example:

```bash
FDL_CATALOG_URL=sqlite:////home/you/.fdl/default/ducklake.sqlite
FDL_CATALOG_PATH=/home/you/.fdl/default/ducklake.sqlite
FDL_DATA_URL=s3://my-bucket/my_dataset/ducklake.duckdb.files/
FDL_DATA_BUCKET=my-bucket
FDL_DATA_PREFIX=my_dataset/ducklake.duckdb.files/
FDL_S3_ENDPOINT=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
FDL_S3_ENDPOINT_HOST=YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
FDL_S3_ACCESS_KEY_ID=...
FDL_S3_SECRET_ACCESS_KEY=...
```

### Usage examples

dlt:

```python
DuckLakeCredentials(
    catalog=os.environ["FDL_CATALOG_URL"],
    storage=os.environ["FDL_DATA_URL"],
)
```

boto3 (S3 targets):

```python
s3.list_objects_v2(
    Bucket=os.environ["FDL_DATA_BUCKET"],
    Prefix=os.environ["FDL_DATA_PREFIX"],
)
```

sqlite3 CLI (catalog inspection):

```bash
sqlite3 "$FDL_CATALOG_PATH" "SELECT * FROM ducklake_snapshots"
```

### Override behavior

Existing environment variables are never overwritten. If `FDL_S3_ENDPOINT` is already set in your shell, fdl will not replace it.

This means in CI/CD, you can set variables explicitly and fdl will respect them:

```yaml
env:
  FDL_S3_ENDPOINT: https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
steps:
  - run: fdl sync default
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

The target must already have a catalog (created by `fdl init` or restored by `fdl pull`). `fdl sql` errors out rather than materializing an empty catalog on demand.

## fdl duckdb

For interactive exploration, `fdl duckdb TARGET` launches the DuckDB CLI with the catalog already attached and selected:

```bash
fdl duckdb default
# D SELECT count(*) FROM cities;
```

Use `--read-only` for safe browsing on shared data. See [`fdl duckdb`](../reference/cli.md#duckdb) for options.

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
