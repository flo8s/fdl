# CLI Reference

fdl provides 8 commands for managing Frozen DuckLake catalogs.

## Commands

| Command | Description |
|---------|-------------|
| [`init`](#init) | Initialize a new project |
| [`pull`](#pull) | Download catalog from target |
| [`push`](#push) | Upload catalog to target |
| [`run`](#run) | Run a command with injected env vars |
| [`sql`](#sql) | Execute SQL against the catalog |
| [`prune`](#prune) | Clean up orphaned data files |
| [`config`](#config) | Get or set configuration |
| [`serve`](#serve) | Start an HTTP server |

## init

Initialize a new project. Creates `fdl.toml`, the `.fdl/` directory, and a DuckLake catalog.

```
fdl init [NAME] [--public-url URL] [--target-url URL] [--target-name NAME] [--sqlite]
```

| Argument / Option | Description |
|---|---|
| `NAME` | Datasource name (default: current directory name) |
| `--public-url` | Public URL for dataset access (prompted if omitted) |
| `--target-url` | Target URL for push/pull (prompted if omitted) |
| `--target-name` | Target name (prompted if omitted, default: `default`) |
| `--sqlite` | Use SQLite catalog (required for [dlt integration](../integrations/dlt.md)) |

When flags are omitted, you'll be prompted interactively.

Generated files:

- `fdl.toml` — Project config (with target settings)
- `.fdl/ducklake.duckdb` — DuckLake catalog (or `.fdl/ducklake.sqlite`)

On failure, `fdl.toml` and `.fdl/` are automatically rolled back.

## pull

Download a catalog from target.

```
fdl pull TARGET
```

| Argument | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |

Requires prior initialization with `fdl init`.

## push

Push catalog to target.

```
fdl push TARGET [--force]
```

| Argument / Option | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `--force`, `-f` | Override conflict detection |

Pushes the DuckLake catalog (`ducklake.duckdb`) and `fdl.toml` to the target. Data files are not included — they are written directly to the target via `fdl run` or `fdl sql`.

SQLite catalogs are automatically converted to DuckDB during push.

### Conflict detection

Before pushing, fdl checks the remote's `.fdl/meta.json` against the local copy. If someone else has pushed since your last pull, push is rejected:

```
Remote was pushed at 2026-04-01T00:00:00+00:00. Run 'fdl pull' first, or use --force to override.
```

Use `--force` to skip this check. The first push to a target with no `.fdl/meta.json` always succeeds.

## run

Run a command with fdl environment variables injected.

```
fdl run TARGET -- COMMAND [ARGS...]
```

| Argument | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `COMMAND` | Command to execute |

See [Working with Data](../guide/working-with-data.md#fdl-run) for details on injected environment variables and usage patterns.

## sql

Execute a SQL query against the DuckLake catalog.

```
fdl sql TARGET QUERY
```

| Argument | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `QUERY` | SQL query to execute |

Examples:

```bash
# Create a table
fdl sql default "CREATE TABLE cities (name VARCHAR, population INTEGER)"

# Insert data
fdl sql default "INSERT INTO cities VALUES ('Tokyo', 14000000), ('Shanghai', 24900000)"

# Query
fdl sql default "SELECT * FROM cities ORDER BY population DESC"
```

See [Working with Data](../guide/working-with-data.md) for details on how the catalog connection works and caveats.

## prune

Clean up orphaned data files on target storage. DuckLake tracks data files by snapshot; files no longer referenced by any active snapshot are considered orphaned.

```
fdl prune TARGET [--dry-run] [--force] [--older-than DAYS]
```

| Argument / Option | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`). S3-compatible targets only. |
| `--dry-run`, `-n` | List orphaned files and sizes without deleting |
| `--force`, `-f` | Skip confirmation prompt |
| `--older-than DAYS` | Only target files older than N days |

The command performs two cleanup steps:

1. Runs DuckLake's `ducklake_cleanup_old_files()` for files scheduled for deletion by snapshot expiration
2. Scans target storage for files not in the active set (`end_snapshot IS NULL`) and deletes them

Deletion is irreversible. Always use `--dry-run` first to review the file list.

Examples:

```bash
# Preview orphaned files and total size
fdl prune default --dry-run

# Delete with confirmation prompt
fdl prune default

# Delete files older than 7 days, no prompt
fdl prune default --older-than 7 --force
```

## config

Get or set configuration.

```
fdl config [KEY] [VALUE]
```

| Argument | Description |
|---|---|
| `KEY` | Config key in `section.name` format (e.g. `targets.default.url`) |
| `VALUE` | Value to set (omit to display current value) |

Reads and writes `fdl.toml`. Without arguments, lists all settings.

Examples:

```bash
# Set target URL
fdl config targets.default.url 's3://${FDL_S3_BUCKET}'
fdl config targets.default.public_url https://data.example.com

# Set S3 credentials with env var references
fdl config targets.default.s3_endpoint '${FDL_S3_ENDPOINT}'
fdl config targets.default.s3_access_key_id '${FDL_S3_ACCESS_KEY_ID}'
fdl config targets.default.s3_secret_access_key '${FDL_S3_SECRET_ACCESS_KEY}'

# List all settings
fdl config
```

See [Configuration](../guide/configuration.md) for details on targets and `${VAR}` expansion.

## serve

Start an HTTP server with CORS and Range request support.

```
fdl serve TARGET [--port PORT]
```

| Argument / Option | Description | Default |
|---|---|---|
| `TARGET` | Target name (e.g. `default`) | — |
| `--port` | Port number | `4001` |

Examples:

```bash
# Serve the default target
fdl serve default

# Serve on a different port
fdl serve default --port 8080
```
