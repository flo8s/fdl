# Why fdl

## The promise of Frozen DuckLake

[Frozen DuckLake](https://ducklake.select/2025/10/24/frozen-ducklake/) is a read-only data lake pattern built on [DuckLake](https://ducklake.select/).
Place a catalog database and Parquet files on object storage — that's all it takes to create a fully functional data lakehouse.

- No database server required — just object storage
- Cost is storage only (reads are free on Cloudflare R2)
- No complex catalog service like Iceberg or Delta Lake
- Anyone can query the data with a single `ATTACH` statement in DuckDB

This means individuals — not just enterprises — can build and publish their own data infrastructure.

## The problem: manual management is painful

In practice, building and maintaining a Frozen DuckLake by hand is tedious:

- Fetching and re-pushing the catalog database requires multiple steps every time
- You must specify storage locations repeatedly, making consistency hard to maintain
- Integrating with tools like dbt scatters configuration across different places
- There is no standard workflow — each project reinvents the process

## What fdl automates

fdl manages the entire Frozen DuckLake lifecycle through a single CLI:

```
fdl init      # Initialize a project
fdl pull      # Fetch catalog from remote
fdl push      # Publish to remote
fdl run       # Execute pipelines with injected config
fdl metadata  # Generate metadata from dbt artifacts
```

- Eliminates repetitive manual steps for catalog management
- Centralizes configuration through a 3-layer system (project → workspace → user)
- Integrates seamlessly with data tools like dbt
- Makes publishing open data as simple as `fdl push`

The goal is to lower the barrier to data infrastructure so that anyone can manage, publish, and share data — advancing the open data ecosystem.

See the [Roadmap](../roadmap.md) for where fdl is headed next.
