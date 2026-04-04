# CLI Reference

## Commands

| Command | Description |
|---------|-------------|
| [`init`](#init) | Initialize a new project |
| [`pull`](#pull) | Download catalog from target |
| [`push`](#push) | Upload catalog to target |
| [`sync`](#sync) | Pull, run pipeline, and push in one step |
| [`run`](#run) | Run a command with injected env vars |
| [`sql`](#sql) | Execute SQL against the catalog |
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
- `.fdl/{target}/ducklake.duckdb` — DuckLake catalog (or `.fdl/{target}/ducklake.sqlite`)

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

## sync

Pull, run a pipeline command, and push — all in one step.

```
fdl sync TARGET [--force]
fdl sync TARGET [--force] -- COMMAND [ARGS...]
```

| Argument / Option | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `--force`, `-f` | Override conflict detection on push |
| `COMMAND` | Command to execute (overrides `command` in fdl.toml) |

When `COMMAND` is omitted, fdl reads `command` from `fdl.toml` — first from `targets.<name>.command`, then from the top-level `command`:

```toml
command = "python main.py"
```

Per-target override:

```toml
command = "python main.py"

[targets.local]
command = "python main.py --quick"
```

```bash
# These are equivalent:
fdl sync default
fdl sync default -- python main.py
```

Processing:

1. Auto-pull if local catalog is missing or stale (same as `fdl run`)
2. Run command with FDL_* environment variables injected
3. Push catalog to target (only on success)

If the command exits with a non-zero code, push is skipped and the exit code is propagated.

See [Working with Data](../guide/working-with-data.md#fdl-sync) for details on injected environment variables and usage patterns.

## run

Run a command with fdl environment variables injected.

```
fdl run TARGET
fdl run TARGET -- COMMAND [ARGS...]
```

| Argument | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `COMMAND` | Command to execute (overrides `command` in fdl.toml) |

When `COMMAND` is omitted, uses `command` from fdl.toml (same lookup as `fdl sync`).

See [Working with Data](../guide/working-with-data.md#injected-variables) for details on injected environment variables.

## sql

Execute a SQL query against the DuckLake catalog.

```
fdl sql TARGET QUERY [--force]
```

| Argument / Option | Description |
|---|---|
| `TARGET` | Target name (e.g. `default`) |
| `QUERY` | SQL query to execute |
| `--force`, `-f` | Skip stale catalog check |

### Stale catalog check

Before executing, fdl verifies that the local catalog is up to date with the remote. If someone else has pushed since your last pull, the command is rejected:

```
Local catalog is stale (remote pushed at 2026-04-01T00:00:00+00:00).
Run 'fdl pull' first, or use --force to override.
```

This prevents queries against outdated data and, for maintenance operations like CHECKPOINT, accidental deletion of active files added by another user's push.

Examples:

```bash
# Create a table
fdl sql default "CREATE TABLE cities (name VARCHAR, population INTEGER)"

# Insert data
fdl sql default "INSERT INTO cities VALUES ('Tokyo', 14000000), ('Shanghai', 24900000)"

# Query
fdl sql default "SELECT * FROM cities ORDER BY population DESC"

# Skip stale catalog check
fdl sql default --force "SELECT * FROM cities"
```

See [Working with Data](../guide/working-with-data.md) for details on how the catalog connection works and caveats.

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
