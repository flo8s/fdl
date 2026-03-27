# Contributing to fdl

## Prerequisites

- Python 3.13 or later
- [uv](https://docs.astral.sh/uv/) for dependency management

## Getting started

```bash
git clone https://github.com/flo8s/fdl.git
cd fdl
uv sync
uv run fdl --help
uv run pytest
```

## Project structure

```
src/fdl/
├── cli.py            — Typer CLI entry point
├── config.py         — 3-layer config resolution (env → workspace → user)
├── config_schema.py  — Configuration schema definitions
├── ducklake.py       — DuckLake catalog operations
├── pull.py           — Pull remote catalog to local
├── push.py           — Push local catalog to remote
├── gc.py             — Garbage collection for orphaned files
├── metadata.py       — dbt artifact metadata generation
├── serve.py          — Local HTTP server with CORS + Range support
├── s3.py             — S3 storage operations
└── console.py        — Console output utilities

packages/fdl_common/  — Shared dbt macros
docs/                 — MkDocs Material documentation source
```

## Commit conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/). Commit messages are parsed by [python-semantic-release](https://python-semantic-release.readthedocs.io/) to determine version bumps and generate the CHANGELOG automatically.

## Releases

Releases are triggered manually via the GitHub Actions "Release" workflow (`workflow_dispatch`). The workflow runs python-semantic-release, which:

1. Reads commit history since the last release
2. Determines the next version based on commit types
3. Updates the version in pyproject.toml
4. Generates CHANGELOG.md entries
5. Creates a Git tag
6. Builds and publishes to PyPI

Do not manually edit the version in pyproject.toml or modify CHANGELOG.md — these are managed by the release automation.

## Documentation

Documentation is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/) and deployed to [fdl.flo8s.com](https://fdl.flo8s.com/).

To preview locally:

```bash
uv run --group docs mkdocs serve
```

Documentation deploys automatically on push to main when files under `docs/`, `mkdocs.yml`, or `CHANGELOG.md` change.

## Code style

- Python 3.13+ — use modern syntax (`X | Y` for unions, etc.)
- All code comments, docstrings, and commit messages in English
