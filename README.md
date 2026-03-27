# fdl

Frozen Data Lake — DuckLake catalog management CLI.

FDL manages the lifecycle of [DuckLake](https://ducklake.select/) catalogs: initialization, building, and distribution.
With a Git-like command interface, you can consistently manage your data catalogs from development to deployment.

## Installation

```bash
pip install frozen-ducklake
```

Requires Python 3.13 or later.

## Quick Start

```bash
# Initialize a project
fdl init my-dataset

# Configure a remote
fdl config remotes.origin s3://my-bucket

# Run the pipeline with injected env vars
fdl run -- dbt run

# Generate metadata from dbt artifacts
fdl metadata

# Push to remote
fdl push origin
```

## Commands

| Command | Description |
|---------|-------------|
| `fdl init` | Initialize a new project |
| `fdl pull` | Download catalog from remote |
| `fdl push` | Upload catalog to remote |
| `fdl metadata` | Generate metadata from dbt artifacts |
| `fdl run` | Run a command with injected env vars |
| `fdl config` | Get or set configuration |
| `fdl serve` | Start an HTTP server |

## Documentation

Full documentation is available at [flo8s.github.io/fdl](https://flo8s.github.io/fdl/).
