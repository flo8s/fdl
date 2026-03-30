# CHANGELOG


## v0.5.1 (2026-03-30)

### Bug Fixes

- Correct formatting in CLI reference table
  ([`e6de3e8`](https://github.com/flo8s/fdl/commit/e6de3e8501dac9b03fd60ac1c5c07fa791aa4334))

### Chores

- Sync uv.lock with pyproject.toml version
  ([`1c4dbea`](https://github.com/flo8s/fdl/commit/1c4dbea558daeba73b95fe8b3e35a66b27b49d71))

### Documentation

- Update README formatting for documentation link
  ([`1fb8e20`](https://github.com/flo8s/fdl/commit/1fb8e2060efb293b39d2ead5d06337ee080abb35))

### Refactoring

- Replace checkpoint command with stale catalog check in fdl sql
  ([`677f23f`](https://github.com/flo8s/fdl/commit/677f23fe15b869a6e22befbff9140ab4f07797fd))


## v0.5.0 (2026-03-29)

### Build System

- Simplify dependencies and add project metadata
  ([`946e2ba`](https://github.com/flo8s/fdl/commit/946e2baf2bcf89aaaec35bc4c005bfb84d46636a))

Remove dbt-core, pydantic, jinja2, pyyaml dependencies. Add MIT license, project URLs, and authors
  to pyproject.toml. Move zensical from docs dependency group to dev.

### Chores

- Remove fdl_common dbt macros package
  ([`605a3d4`](https://github.com/flo8s/fdl/commit/605a3d403ce90ebd94c36318faa39ac16b99301e))

### Continuous Integration

- Update docs workflow to use zensical
  ([`330ab5c`](https://github.com/flo8s/fdl/commit/330ab5cc90fbfe752a7cb26d2e1bcc0d28b27594))

- Update docs workflow to use zensical
  ([`420b05b`](https://github.com/flo8s/fdl/commit/420b05b0310ebeef0bf0618a3516f16f34345f11))

### Documentation

- Add concept pages and global install instructions
  ([`35180ad`](https://github.com/flo8s/fdl/commit/35180ad2ecb0c4e3404267cd7ad148b570d2dd46))

- Add "Why fdl" page explaining the Frozen DuckLake pattern and motivation - Add Roadmap page with
  vision toward a package manager for Frozen DuckLake - Add global install instructions (uv tool /
  pipx) to quickstart

- Migrate from mkdocs to zensical
  ([`5b8359c`](https://github.com/flo8s/fdl/commit/5b8359c4e599bceeecd7b5f3fb24403746e3ccda))

Replace mkdocs-material with zensical as the documentation engine. Replace changelog symlink with
  pymdownx.snippets include.

- Rewrite documentation for target-based architecture
  ([`e48b8e3`](https://github.com/flo8s/fdl/commit/e48b8e3df00d07b30042f0a3f430e762fb460690))

Restructure docs to match new target-based config model. Migrate from mkdocs to zensical. Add guide,
  reference, and resources sections with new content.

- Update CLAUDE.md and add CONTRIBUTING.md
  ([`28d12f1`](https://github.com/flo8s/fdl/commit/28d12f1afe85d94f2249ca36c9b6c54b2c8e95fa))

- Update README, CLAUDE.md, and CONTRIBUTING.md
  ([`0c48dff`](https://github.com/flo8s/fdl/commit/0c48dff5f858e2a334a8c6744020ccb232949c85))

### Features

- Add --version flag, improve gc output
  ([`2046a11`](https://github.com/flo8s/fdl/commit/2046a11e5fdd8d6dc01949bc58ec01129bd200b0))

- Add push conflict detection and --force flag
  ([`3db0216`](https://github.com/flo8s/fdl/commit/3db0216650bc4b78479230c376de61cc37eab534))

- Track pushed_at timestamp in .fdl/meta.json (local + remote) - Push rejects when remote was
  updated since last pull - --force overrides conflict detection - Pull syncs meta.json from remote
  for next push check

- Rewrite to target-based architecture
  ([`a17d8ec`](https://github.com/flo8s/fdl/commit/a17d8ecd6648681b0acf5426f232ae00252f5abe))

Replace 3-layer config (env → workspace → user) with single fdl.toml and ${VAR} environment variable
  expansion.

BREAKING CHANGE: - `remote` renamed to `target` across all commands - `FDL_ATTACH_PATH` renamed to
  `FDL_CATALOG` - `gc` command renamed to `prune` - `fdl run` now requires TARGET argument -
  `metadata` command removed - `create_destination()` removed from ducklake module - User/workspace
  config files (~/.fdl/config, .fdl/config) no longer read

New features: - `fdl sql TARGET QUERY` — execute SQL against the catalog - `fdl init` prompts
  interactively for target config - S3Config dataclass for typed credential handling

### Refactoring

- Make load_toml strict and remove datasource_name fallback
  ([`63ca8ed`](https://github.com/flo8s/fdl/commit/63ca8ed895b6bfde79b460dcb6afa209cd1ab958))

- load_toml → _load_toml: internal function, raises FileNotFoundError - set_value: catch
  FileNotFoundError for new file creation - datasource_name: require name in fdl.toml, no directory
  name fallback

- Replace print with rich console output
  ([`3958b6b`](https://github.com/flo8s/fdl/commit/3958b6b727226cfc4ca85fcc846136927549c387))

- Replace prune with checkpoint command
  ([`6433bcd`](https://github.com/flo8s/fdl/commit/6433bcdd3c59ace27cea5588e57956477d069c7a))

- Use DuckLake CHECKPOINT statement via connect() context manager - Remove S3-only restriction,
  works on any target - Extract is_stale pure function and read_remote_pushed_at to meta.py - Add
  stale catalog check before maintenance (reuses push conflict detection)

- Unify branding to Frozen DuckLake
  ([`cd148ca`](https://github.com/flo8s/fdl/commit/cd148ca6ff0f745ef881cabe8173d64e694cee98))

- Rename "Frozen Data Lake" to "Frozen DuckLake" across all files - Normalize casing: FDL → fdl -
  Remove "Git-like" messaging - Update site_name to "fdl — Frozen DuckLake CLI"

### Testing

- Add FDL_DATA_PATH injection test
  ([`2644ae9`](https://github.com/flo8s/fdl/commit/2644ae9378446c89f6bdbcfaa02187bd1cf479e1))

Verify fdl run injects FDL_DATA_PATH ending with ducklake.duckdb.files/

- Add integration and unit tests (89 tests)
  ([`1d01ffe`](https://github.com/flo8s/fdl/commit/1d01ffeec8e2f0580bc6310c36b8a171b6e3bb23))

Integration tests (tests/integration/): - test_init: default catalog, invalid name, existing toml,
  double init, rollback, sqlite - test_config: get, set, list-all, env var reference, s3
  credentials, missing key, without init - test_sql: data persistence, invalid sql - test_run: env
  injection, no-overwrite, separator, exit code propagation - test_push: catalog copy, sqlite
  conversion, meta.json, conflict detection, force push - test_pull: catalog restore, meta.json
  sync, pull-then-push, empty target - test_prune: local target rejection, orphan deletion, dry-run
  - test_serve: CORS, range request, HEAD, OPTIONS, 404

Unit tests (tests/): - test_config: set_value, get_all, datasource_name, storage/data_path/
  catalog_path, resolve_target, target_s3_config, ducklake_url, fdl_env_dict - test_init_module:
  ducklake_data_path, default_target_url - test_s3: S3Config endpoint_host - test_serve:
  CORSRangeHandler

- Add Phase 1 pure function tests
  ([`a118610`](https://github.com/flo8s/fdl/commit/a118610fb8348579555c945967c9020de3ab6ebe))

- test_init_module.py: ducklake_data_path, default_target_url - test_s3.py: S3Config endpoint_host

- Add S3 target integration tests for push and pull
  ([`c6caf1d`](https://github.com/flo8s/fdl/commit/c6caf1ddea4a0fa15c0faaab8bc270e6024bf808))

- push uploads catalog to S3 - push conflict detection on S3 - pull restores catalog from S3


## v0.4.0 (2026-03-27)

### Continuous Integration

- Add GitHub Pages deployment workflow
  ([`ebad8ba`](https://github.com/flo8s/fdl/commit/ebad8ba244a1846a626df2da03db59c84a9d57b4))

Build and deploy docs on push to main (docs/**, mkdocs.yml, CHANGELOG.md) and manual dispatch.

### Documentation

- Add MkDocs Material documentation site
  ([`7f00206`](https://github.com/flo8s/fdl/commit/7f00206a60c1eb6cf098d5d80db8283c5ed4a051))

- mkdocs.yml with Material theme (light/dark, code copy, search) - 6 pages: index, quickstart, CLI
  reference, configuration, dlt integration, changelog - Changelog page symlinked to CHANGELOG.md -
  Update README.md with project overview and commands - Add site/ to .gitignore

- Update GitHub Pages actions and set custom site URL
  ([`5fe951b`](https://github.com/flo8s/fdl/commit/5fe951b03bc0cdac6aca1e890c2ded9026066dca))

### Features

- Add fdl gc command with --dry-run support
  ([`9e5d451`](https://github.com/flo8s/fdl/commit/9e5d451f53c1ffa488ecab564c6a0460d32f859b))

fdl gc origin --dry-run # list orphaned files and sizes fdl gc origin # interactive deletion with
  confirmation fdl gc origin --force # delete without confirmation fdl gc origin --older-than 7 #
  only files older than 7 days


## v0.3.3 (2026-03-27)

### Bug Fixes

- Include fdl.toml in config value resolution
  ([`2e6c921`](https://github.com/flo8s/fdl/commit/2e6c921e4835f94d54f47ff889809abf1f2ec5dc))

_get_config_value now checks project config (fdl.toml) before workspace and user config, matching
  resolve_remote's 3-layer lookup.


## v0.3.2 (2026-03-26)

### Bug Fixes

- Expand env vars in remote URLs
  ([`38341e3`](https://github.com/flo8s/fdl/commit/38341e30a2c131630b4eecfa96d28e0a84ca841c))

resolve_remote() now calls os.path.expandvars() so that fdl.toml remotes like s3://${FDL_S3_BUCKET}
  are expanded.


## v0.3.1 (2026-03-26)

### Bug Fixes

- Remove .fdl/ pre-check from pull command
  ([`4e2023b`](https://github.com/flo8s/fdl/commit/4e2023be8cf8f9e2d6d513014855134d6d520fd7))

pull.py already creates .fdl/ via mkdir(parents=True). The check broke CI where .fdl/ is gitignored
  and doesn't exist before pull.


## v0.3.0 (2026-03-26)

### Chores

- Bump frozen-ducklake to 0.2.1
  ([`b359533`](https://github.com/flo8s/fdl/commit/b3595338fa0651506c8778d32f094b7f9385d1df))

### Features

- Require fdl init before pull, remove --sqlite fallback
  ([`b21369e`](https://github.com/flo8s/fdl/commit/b21369e0d06a9977e4e2d4bba90661f551dcd01d))

pull no longer auto-initializes the catalog. Users must run fdl init first. Removes --sqlite option
  from pull.

- Rewrite fdl init with fdl.toml scaffolding and rollback
  ([`e5e6454`](https://github.com/flo8s/fdl/commit/e5e645457f4ed0ac65cbc04f9909fee9bc43805c))

- name argument required (like git init <repo>) - generates fdl.toml with name and optional catalog
  type - auto-creates .gitignore entry for .fdl/ - rolls back fdl.toml and .fdl/ on failure -
  set_value now auto-detects top-level vs sectioned keys

### Refactoring

- Move datasource/URL resolution from DatasetConfig to config module
  ([`6ff1ff2`](https://github.com/flo8s/fdl/commit/6ff1ff224eac627954cc4dd702cdae6f855eec09))

datasource_name(), public_url(), ducklake_url() are now in config.py with 3-layer resolution (env
  var → workspace → user config). DatasetConfig no longer holds public_url or ducklake_url.

- Resolve storage in create_destination, add FDL_S3_ENDPOINT_HOST
  ([`26bcec8`](https://github.com/flo8s/fdl/commit/26bcec89dcfc0a946602f2db9ef5c6ad96325091))

- create_destination defaults to config.storage() instead of hardcoded .fdl - s3_env_dict now
  derives FDL_S3_ENDPOINT_HOST (scheme-less) for DuckDB


## v0.2.1 (2026-03-26)

### Bug Fixes

- Update outdated docstring in create_destination
  ([`ad31665`](https://github.com/flo8s/fdl/commit/ad31665cf4cf9cd02bd4649ac45f16c2153629f4))

### Continuous Integration

- Merge publish into release workflow
  ([`22ba582`](https://github.com/flo8s/fdl/commit/22ba582fab145d68240605c9b68a9571492e6161))

GITHUB_TOKEN tags don't trigger other workflows. Run PyPI publish in the same job after
  semantic-release.

- Switch release to manual trigger (workflow_dispatch)
  ([`0186734`](https://github.com/flo8s/fdl/commit/01867344bcb931c7436407caefbf522afeaba22f))


## v0.2.0 (2026-03-26)

### Bug Fixes

- Allow zero version in semantic-release
  ([`074baae`](https://github.com/flo8s/fdl/commit/074baaea2e41eff15f74758c8d1e094d0d1e87d1))

- Use major_on_zero=false to stay in 0.x
  ([`edbc322`](https://github.com/flo8s/fdl/commit/edbc32267ec828ab21891981e286bc42a230d93d))

### Chores

- Update uv.lock
  ([`1331865`](https://github.com/flo8s/fdl/commit/1331865a39543392f2b74a83ce0fcc476259b1f4))

### Features

- Named remotes, fdl run/config/serve, FDL_* env vars
  ([`d749935`](https://github.com/flo8s/fdl/commit/d749935fafa493f6d5921876a51485c58f06b338))

BREAKING CHANGE: push/pull now require named remotes instead of URLs. s3_url removed from
  dataset.yml, replaced by fdl.toml remotes. DUCKLAKE_STORAGE renamed to FDL_STORAGE. S3 env vars
  prefixed with FDL_S3_.

Named remotes: - push/pull require explicit remote name (e.g. origin, local) - Remotes defined in
  fdl.toml (project), .fdl/config (workspace), ~/.fdl/config (user) - No built-in remotes; all
  user-defined via fdl config

fdl run: - Sets FDL_STORAGE, FDL_DATA_PATH, FDL_ATTACH_PATH for pipeline execution - S3 credentials
  loaded from config (env var → workspace → user) - Usage: fdl run [REMOTE] -- COMMAND

fdl config: - git config-like settings management - fdl config key value (user), fdl config --local
  key value (workspace)

fdl serve: - HTTP server with CORS + Range request support - Optional remote arg: fdl serve (project
  .fdl/) or fdl serve REMOTE

Other changes: - DIST_DIR renamed to FDL_DIR - S3 endpoint now stored with https:// scheme - gc.py:
  add OVERRIDE_DATA_PATH for v1.0 compatibility

### Breaking Changes

- Push/pull now require named remotes instead of URLs. s3_url removed from dataset.yml, replaced by
  fdl.toml remotes. DUCKLAKE_STORAGE renamed to FDL_STORAGE. S3 env vars prefixed with FDL_S3_.


## v0.1.0 (2026-03-22)

### Features

- Replace tagpr with python-semantic-release
  ([`c8e72ed`](https://github.com/flo8s/fdl/commit/c8e72eda39bc0cf978c9f32914b5122c6ab5dd24))

- Update release workflow and remove changelog
  ([`76eceb8`](https://github.com/flo8s/fdl/commit/76eceb876fbb0d262ba62c774077847c8fa60c62))
