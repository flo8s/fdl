# Known Issues

## DATA_PATH is fixed at init time

The catalog's `DATA_PATH` is set during `fdl init` based on the `public_url` you provide. fdl does not currently update it automatically when you change `public_url` in the target config.

This means:

- If you init with `public_url = http://localhost:4001`, the catalog expects data files at `http://localhost:4001/<dataset>/ducklake.duckdb.files/`
- To deploy to a different public URL (e.g. `https://data.example.com`), you need to either re-init or manually update the catalog
- Changing `public_url` via `fdl config` does NOT update the catalog's `DATA_PATH`

### Workaround 1: Re-initialize

```bash
rm -rf .fdl
fdl init my_dataset --public-url https://data.example.com --target-url s3://your-bucket
```

This deletes the local catalog and data. Pull from the target after re-init if needed.

### Workaround 2: Update catalog directly

You can modify `DATA_PATH` in the DuckLake catalog's internal metadata table:

```python
import duckdb

conn = duckdb.connect(".fdl/default/ducklake.duckdb")
conn.execute("""
    UPDATE ducklake_metadata
    SET value = 'https://data.example.com/my_dataset/ducklake.duckdb.files/'
    WHERE key = 'data_path'
""")
conn.close()
```

This preserves your data but relies on DuckLake internal tables, which may change in future versions. fdl pins `duckdb>=1.4.0,<1.5.0` to mitigate this risk.

### Future

DuckLake v1.0 may introduce an official API for modifying `DATA_PATH`, which would allow fdl to handle this automatically at push time.

Related:

- [OVERRIDE_DATA_PATH for Frozen DuckLake](https://github.com/duckdb/ducklake/issues/580)
- [DATA_PATH re-attach consistency](https://github.com/duckdb/ducklake/issues/218)
- [DuckLake DATA_PATH discussion](https://github.com/duckdb/ducklake/discussions/695)

## All commands require an explicit target

`fdl sql`, `fdl serve`, `fdl push`, `fdl pull`, and `fdl run` all require an explicit target name (e.g. `default`). There is no implicit "local" mode.

This is intentional:

- The catalog's `DATA_PATH` points to the public URL, not `.fdl/`. Writing data to `.fdl/` would create a mismatch between where the catalog expects data and where it actually is.
- All data writes go directly to the target storage (XDG_DATA_HOME by default, or S3-compatible storage). The `.fdl/` directory only holds the catalog.
- This keeps the model simple: one target, one storage location, no hidden behavior.

DuckLake v1.0 may enable writing to a local workspace and rewriting `DATA_PATH` at push time, which could allow implicit local mode in the future.
