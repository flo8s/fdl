# CHANGELOG


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
