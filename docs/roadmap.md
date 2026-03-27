# Roadmap

### Now — Lifecycle Management

fdl automates the build-and-deploy cycle for Frozen DuckLake catalogs.

- `init` / `pull` / `push` / `run` / `metadata` / `config` / `serve`
- 3-layer configuration (project → workspace → user)
- dbt integration for pipeline execution and metadata generation

### Next — Versioning

DuckLake supports [snapshots and time travel](https://ducklake.select/2025/10/24/frozen-ducklake/#versioning).
fdl will wrap these primitives to provide catalog versioning — publish new versions, pin to specific snapshots, and query historical data.

### Next — Dataset Dependencies

Declare other datasets as dependencies and let fdl handle `ATTACH` automatically.
This enables composable data pipelines where datasets can build on top of each other.

### Future — Dataset Registry

A shared registry for publishing and discovering datasets — making open data as easy to share as open source code.
