# Working with Data

## Live and frozen catalogs

fdl separates two roles:

- **Live catalog** (`[metadata]` + `[data]`) — where writes land. SQLite for single-writer setups, PostgreSQL for concurrent writers across hosts.
- **Frozen snapshot** (`[publishes.<name>]`) — read-only DuckDB catalog uploaded to an HTTPS/S3 location for consumers.

`fdl clone` is the inbound direction (frozen → live). `fdl publish` is the outbound direction (live → frozen).

## fdl run

`fdl run` executes your pipeline and publishes the result in one step.

```bash
fdl run                      # uses command from fdl.toml
fdl run NAME                  # publishes to [publishes.NAME]
fdl run [NAME] -- COMMAND     # explicit command
```

Define the default command in `fdl.toml`:

```toml
command = "python main.py"
```

Per-publish overrides are also supported (via `publishes.<name>.command`).

### Semantics

1. Resolve the command (CLI arg → `publishes.<name>.command` → top-level `command`)
2. Run the subprocess with FDL_* environment variables injected
3. On success, publish to the resolved destination

If the command exits non-zero, the publish step is skipped and the exit code is returned unchanged. If `[publishes]` is empty, publish is simply skipped (pure-execution mode).

### Injected variables

| Variable | Description | Presence |
|---|---|---|
| `FDL_CATALOG_URL` | Live catalog URL verbatim from `[metadata].url` | always |
| `FDL_DATA_URL` | `[data].url` verbatim | always |
| `FDL_CATALOG_PATH` | Absolute path to the sqlite file | sqlite metadata only |
| `FDL_DATA_BUCKET` | S3 bucket (parsed from `[data].url`) | s3 data only |
| `FDL_DATA_PREFIX` | S3 key prefix | s3 data only |
| `FDL_S3_ENDPOINT` / `FDL_S3_ENDPOINT_HOST` | S3 endpoint | s3 data only |
| `FDL_S3_ACCESS_KEY_ID` / `FDL_S3_SECRET_ACCESS_KEY` | S3 credentials | s3 data only |

`FDL_CATALOG_URL` is what DuckLake / dlt / dbt-ducklake expect as a connection string.

### Override behavior

Existing environment variables are never overwritten. If `FDL_S3_ENDPOINT` is already set in your shell, fdl will not replace it.

## fdl sql

Execute SQL directly against the live catalog:

```bash
fdl sql "CREATE TABLE cities (name VARCHAR, population INTEGER)"
fdl sql "INSERT INTO cities VALUES ('Tokyo', 14000000)"
fdl sql "SELECT * FROM cities"
```

`fdl sql` opens a DuckDB connection, attaches the live catalog, executes the query, and closes. Data is written directly to `[data].url` on S3 or local disk.

Each call is a separate connection — no transactions across multiple `fdl sql` invocations.

## fdl duckdb

For interactive exploration, `fdl duckdb` launches the DuckDB CLI with the live catalog attached and selected:

```bash
fdl duckdb
# D SELECT count(*) FROM cities;
```

Use `--read-only` for safe browsing on shared data.

## fdl publish / fdl clone

Publish a frozen snapshot and serve it:

```bash
fdl publish                  # uploads frozen DuckDB + fdl.toml to publishes.<name>.url
fdl serve                    # HTTP-serves the local publish directory
```

Clone a published catalog to start editing:

```bash
fdl clone https://data.example.com/my_dataset/
fdl sql "SELECT count(*) FROM cities"
```

`fdl serve` starts an HTTP server with CORS and Range request support — everything DuckDB needs to `ATTACH` a remote DuckLake catalog.
