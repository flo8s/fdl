# CLAUDE.md

## Development

```bash
uv sync
uv run fdl --help
uv run pytest
uv run zensical serve
```

## Code style

- Python 3.13+ — use modern syntax (`X | Y` unions, etc.)
- Write all code comments and docstrings in English
- Commit messages in English

## Commit conventions

Follow [Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/).
python-semantic-release がコミットメッセージからバージョンを自動決定する。

リリースに影響するタイプ:
- `fix:` → パッチバージョン (0.5.0 → 0.5.1)
- `feat:` → マイナーバージョン (0.5.0 → 0.6.0)
- `!` 付き (例: `refactor!:`, `feat!:`) → マイナーバージョン (major_on_zero=false のため)
- `refactor:`, `docs:`, `chore:`, `test:`, `ci:` など → リリースなし

破壊的変更 (コマンド削除、API変更、設定形式変更) は必ず `!` を付けるか、フッターに `BREAKING CHANGE:` を記載する。

## Releases

Releases are handled by python-semantic-release via GitHub Actions (manual workflow_dispatch). Do not manually edit the version in pyproject.toml or modify CHANGELOG.md.

## Project structure

- `src/fdl/` — main package (cli.py, config.py, ducklake.py, pull.py, push.py, etc.)
- `docs/` — Zensical documentation source
