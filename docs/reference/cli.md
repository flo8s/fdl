# CLI Reference

All commands (except `init` and `clone`) discover `fdl.toml` by walking up from the current working directory, so they can be run from any subdirectory of your project. The same lookup is also used by the [Python API](python-api.md).

## Commands

| Command | Description |
|---------|-------------|
| [`init`](#init) | Initialize a new project |
| [`clone`](#clone) | Clone a published frozen DuckLake into a new project |
| [`publish`](#publish) | Convert the live catalog to frozen DuckDB and upload |
| [`run`](#run) | Run a pipeline command and publish on success |
| [`sql`](#sql) | Execute SQL against the live catalog |
| [`duckdb`](#duckdb) | Launch an interactive DuckDB shell |
| [`config`](#config) | Get or set configuration |
| [`serve`](#serve) | Serve a local publish directory over HTTP |

## init

Initialize a new project. Writes `fdl.toml` and provisions the live catalog.

```
fdl init [NAME] [--metadata-url URL] [--data-url URL] [--publish-url URL] [--publish-name NAME]
```

| Argument / Option | Description |
|---|---|
| `NAME` | Datasource name (default: current directory name) |
| `--metadata-url` | Live catalog URL (default: `sqlite:///./.fdl/ducklake.sqlite`) |
| `--data-url` | Data storage URL (default: `./.fdl/data`) |
| `--publish-url` | Optional publish destination |
| `--publish-name` | Name of the `[publishes.*]` entry (default: `default`) |

### Postgres metadata

When `--metadata-url` is a `postgres://` URL, the target database must already exist. fdl creates the schema via `CREATE SCHEMA IF NOT EXISTS`; it does not run `CREATE DATABASE` (which requires superuser privileges and runs outside a transaction).

## clone

Clone a published frozen DuckLake into a new local live catalog.

```
fdl clone URL [--force]
```

| Argument / Option | Description |
|---|---|
| `URL` | Base URL of a published catalog |
| `--force`, `-f` | Overwrite an existing local fdl.toml / catalog |

Expects both `<URL>/fdl.toml` and `<URL>/ducklake.duckdb` to be fetchable at the given base. Works with HTTPS, `s3://`, and local paths.

## publish

Convert the live catalog to a frozen DuckDB snapshot and upload it.

```
fdl publish [NAME] [--force]
```

| Argument / Option | Description |
|---|---|
| `NAME` | Publish name (default: the sole `[publishes.*]` entry) |
| `--force`, `-f` | Override ETag precondition on S3 uploads |

### Conflict detection (S3)

For S3 destinations, fdl uploads the catalog with an HTTP `If-Match` precondition using the ETag recorded from the previous publish. If another client has published since, the S3 server rejects the upload with `412 Precondition Failed`:

```
Remote has been updated since the last publish. Run 'fdl clone --force' first, or pass --force to override.
```

The first publish uses `If-None-Match: *`, which succeeds only when no catalog is present. Use `--force` to skip the precondition. Local destinations are assumed single-user and skip conflict detection.

## run

Run a pipeline command and publish on success.

```
fdl run                      # uses command from fdl.toml
fdl run NAME                  # publishes to [publishes.NAME]
fdl run [NAME] -- COMMAND    # explicit command
```

| Argument | Description |
|---|---|
| `NAME` | Publish name (default: sole entry, or none for execution-only) |
| `COMMAND` | Command to execute (overrides `command` in fdl.toml) |

When `COMMAND` is omitted, fdl reads `command` from `fdl.toml` — first from `publishes.<name>.command`, then from the top-level `command`:

```toml
command = "python main.py"
```

### Semantics

- If the command exits non-zero, publish is skipped and the exit code is returned.
- If `[publishes]` has exactly one entry, it is used implicitly.
- If `[publishes]` is empty, publish is skipped (pure execution).
- If `[publishes]` has multiple entries, an explicit `NAME` is required.

The subprocess runs with the project root (the directory containing `fdl.toml`) as its working directory. FDL_* environment variables are injected (see [Working with Data](../guide/working-with-data.md#injected-variables)).

## sql

Execute a SQL query against the live DuckLake catalog.

```
fdl sql QUERY
```

Requires a reachable live catalog (`fdl init` or `fdl clone` first). For postgres metadata, the database must be reachable.

Examples:

```bash
fdl sql "CREATE TABLE cities (name VARCHAR, population INTEGER)"
fdl sql "INSERT INTO cities VALUES ('Tokyo', 14000000), ('Shanghai', 24900000)"
fdl sql "SELECT * FROM cities ORDER BY population DESC"
```

## duckdb

Launch an interactive DuckDB shell with the live catalog attached and selected.

```
fdl duckdb [--read-only] [--dry-run] [--duckdb-bin PATH]
```

| Option | Description |
|---|---|
| `--read-only` | Attach the catalog in read-only mode |
| `--dry-run` | Print the duckdb command that would be executed and exit |
| `--duckdb-bin` | Path to the `duckdb` binary (default: first on PATH) |

fdl `exec`s into the `duckdb` CLI with `INSTALL ducklake`, `ATTACH`, and `USE` pre-applied via `-cmd`. TTY, signal handling, and the exit code are inherited normally.

For S3 data storage, `httpfs` is loaded and `CREATE SECRET` is issued from `[data].s3_*` in `fdl.toml`. Postgres metadata causes `postgres` extension to be loaded.

Requires the `duckdb` CLI on `PATH` (or pass `--duckdb-bin`).

## config

Get or set configuration.

```
fdl config [KEY] [VALUE]
```

| Argument | Description |
|---|---|
| `KEY` | Config key in `section.name` format (e.g. `metadata.url`) |
| `VALUE` | Value to set (omit to display current value) |

Reads and writes `fdl.toml`. Without arguments, lists all settings.

Examples:

```bash
fdl config metadata.url 'sqlite:///./.fdl/ducklake.sqlite'
fdl config data.url 's3://${FDL_S3_BUCKET}/my_dataset'
fdl config publishes.default.url 'https://data.example.com'
fdl config
```

## serve

Serve a local publish directory over HTTP with CORS and Range request support.

```
fdl serve [NAME] [--port PORT]
```

| Argument / Option | Description | Default |
|---|---|---|
| `NAME` | Publish name (default: sole `[publishes.*]` entry) | — |
| `--port` | Port number | `4001` |

Only works when `publishes.<name>.url` is a local path. Remote URLs (`s3://`, `http(s)://`) are rejected with a clear error.
