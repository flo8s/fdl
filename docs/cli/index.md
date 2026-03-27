# CLI Reference

FDL provides 7 commands with a Git-like interface.

## Commands

| Command | Description | Git analogy |
|---------|-------------|-------------|
| [`init`](#init) | Initialize a new project | `git init` |
| [`pull`](#pull) | Download catalog from remote | `git pull` |
| [`push`](#push) | Upload catalog to remote | `git push` |
| [`metadata`](#metadata) | Generate metadata from dbt artifacts | — |
| [`run`](#run) | Run a command with injected env vars | — |
| [`config`](#config) | Get or set configuration | `git config` |
| [`serve`](#serve) | Start an HTTP server | — |

## init

Initialize a new project. Creates `fdl.toml`, the `.fdl/` directory, and a DuckLake catalog.

```
fdl init NAME [--sqlite]
```

| Argument / Option | Description |
|---|---|
| `NAME` | Datasource name (required) |
| `--sqlite` | Use SQLite catalog (required for [dlt integration](../integrations/dlt.md)) |

Generated files:

- `fdl.toml` — Project config
- `.fdl/ducklake.duckdb` — DuckLake catalog (or `.fdl/ducklake.sqlite`)
- `.gitignore` — `.fdl/` entry added

On failure, `fdl.toml` and `.fdl/` are automatically rolled back.

## pull

Download a catalog from remote.

```
fdl pull SOURCE
```

| Argument | Description |
|---|---|
| `SOURCE` | Remote name (e.g. `origin`) |

Requires prior initialization with `fdl init`.

## push

Upload build artifacts to remote.

```
fdl push DEST
```

| Argument | Description |
|---|---|
| `DEST` | Remote name (e.g. `origin`, `local`) |

Uploaded files:

- `ducklake.duckdb` — DuckLake catalog
- `ducklake.duckdb.files/` — Data files (Parquet, etc.)
- `metadata.json` — Metadata
- `docs/` — dbt documentation

SQLite catalogs are automatically converted to DuckDB during push.

## metadata

Generate metadata from dbt artifacts.

```
fdl metadata [TARGET_DIR]
```

| Argument | Description | Default |
|---|---|---|
| `TARGET_DIR` | dbt target directory path | `target` |

Parses `manifest.json` and `catalog.json` to generate `.fdl/metadata.json`.
dbt documentation (`docs/`) is also copied to `.fdl/docs/`.

Requires `dataset.yml` in the project root.

## run

Run a command with FDL environment variables injected.

```
fdl run [REMOTE] -- COMMAND [ARGS...]
```

| Argument | Description |
|---|---|
| `REMOTE` | Remote name (omit for local `.fdl/`) |
| `COMMAND` | Command to execute |

Injected environment variables:

| Variable | Description |
|---|---|
| `FDL_STORAGE` | Base storage path |
| `FDL_DATA_PATH` | Data files path |
| `FDL_ATTACH_PATH` | DuckLake attach path (for dbt) |
| `FDL_S3_ENDPOINT` | S3 endpoint |
| `FDL_S3_ACCESS_KEY_ID` | S3 access key |
| `FDL_S3_SECRET_ACCESS_KEY` | S3 secret key |
| `FDL_S3_ENDPOINT_HOST` | S3 endpoint without scheme |

Existing environment variables are never overwritten.

Examples:

```bash
# Use local .fdl/ for dbt run
fdl run -- dbt run

# Use remote storage for dbt run
fdl run origin -- dbt run
```

## config

Get or set configuration. Works like `git config`.

```
fdl config [--list] [--local] [KEY] [VALUE]
```

| Argument / Option | Description |
|---|---|
| `KEY` | Config key in `section.name` format (e.g. `s3.endpoint`) |
| `VALUE` | Value to set (omit to display current value) |
| `--list`, `-l` | List all settings |
| `--local` | Write to project config (`fdl.toml`) |

Default write target is `~/.fdl/config` (user level).
With `--local`, writes to `.fdl/config` (workspace level).

Examples:

```bash
# Register a remote
fdl config --local remotes.origin s3://my-bucket

# Set S3 credentials
fdl config s3.endpoint https://r2.cloudflarestorage.com
fdl config s3.access_key_id YOUR_KEY
fdl config s3.secret_access_key YOUR_SECRET

# List all settings
fdl config --list
```

See [Configuration](../concepts/configuration.md) for details on config resolution order.

## serve

Start an HTTP server with CORS and Range request support.

```
fdl serve [REMOTE] [--port PORT]
```

| Argument / Option | Description | Default |
|---|---|---|
| `REMOTE` | Remote name (omit for project's `.fdl/`) | — |
| `--port` | Port number | `4001` |

Examples:

```bash
# Serve current project's .fdl/
fdl serve

# Serve all datasets from a local remote
fdl serve local --port 8080
```
