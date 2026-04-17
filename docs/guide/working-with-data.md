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

Options:

| Option | Description |
|---|---|
| `--force`, `-f` | Override conflict detection on push |

### Injected variables

The pipeline command receives these environment variables:

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
