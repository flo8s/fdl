# Known Issues

## All commands require an explicit target

`fdl sql`, `fdl serve`, `fdl push`, `fdl pull`, and `fdl run` all require an explicit target name (e.g. `default`). There is no implicit "local" mode.

This is intentional:

- The catalog's `DATA_PATH` points to the public URL, not `.fdl/`. Writing data to `.fdl/` would create a mismatch between where the catalog expects data and where it actually is.
- All data writes go directly to the target storage (XDG_DATA_HOME by default, or S3-compatible storage). The `.fdl/` directory only holds the catalog.
- This keeps the model simple: one target, one storage location, no hidden behavior.

DuckLake v1.0 may enable writing to a local workspace and rewriting `DATA_PATH` at push time, which could allow implicit local mode in the future.
