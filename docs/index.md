# FDL

DuckLake catalog management CLI.

FDL manages the lifecycle of [DuckLake](https://ducklake.select/) catalogs — initialization, building, and distribution.
With a Git-like command interface, you can consistently manage your data catalogs from development to deployment.

## Features

- Initialize and manage DuckLake catalogs (`fdl init`)
- Push and pull to S3 or local storage via Named Remotes
- 3-layer configuration management (project → workspace → user)
- Automatic metadata generation from dbt artifacts
- Environment variable injection for pipeline execution (`fdl run`)
- HTTP server with CORS and Range request support (`fdl serve`)

## Installation

=== "uv"

    ```bash
    uv add frozen-ducklake
    ```

=== "pip"

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

# Run the pipeline
fdl run -- dbt run

# Generate metadata
fdl metadata

# Upload to remote
fdl push origin
```

See [Quick Start](getting-started/quickstart.md) for a complete walkthrough.

## Learn More

- [Quick Start](getting-started/quickstart.md) — End-to-end setup and deployment guide
- [Configuration](concepts/configuration.md) — 3-layer config management and environment variables
- [CLI Reference](cli/index.md) — All available commands
