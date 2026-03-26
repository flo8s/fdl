# CHANGELOG


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
