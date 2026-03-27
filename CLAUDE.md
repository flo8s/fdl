# CLAUDE.md

## Development

```bash
uv sync
uv run fdl --help
uv run pytest
uv run --group docs zensical serve
```

## Code style

- Python 3.13+ — use modern syntax (`X | Y` unions, etc.)
- Write all code comments and docstrings in English
- Commit messages in English

## Commit conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/).

## Releases

Releases are handled by python-semantic-release via GitHub Actions (manual workflow_dispatch). Do not manually edit the version in pyproject.toml or modify CHANGELOG.md.

## Project structure

- `src/fdl/` — main package (cli.py, config.py, ducklake.py, pull.py, push.py, gc.py, etc.)
- `packages/fdl_common/` — shared dbt macros
- `docs/` — Zensical documentation source
