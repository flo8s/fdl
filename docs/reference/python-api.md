# Python API Reference

fdl exposes the same operations available in the [CLI](cli.md) as a Python
package, so pipelines and orchestrators (e.g. Dagster, Airflow) can drive
DuckLake catalogs without spawning a subprocess.

## At a glance

```python
import fdl

fdl.init("mydata", target_url="s3://my-bucket")
fdl.pull("default")

with fdl.connect("default") as conn:
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")

fdl.push("default")
```

## Function-to-command mapping

| Python API | CLI |
|---|---|
| `fdl.init(name, ...)` | `fdl init NAME` |
| `fdl.pull(target)` | `fdl pull TARGET` |
| `fdl.push(target)` | `fdl push TARGET` |
| `fdl.run(target, command)` | `fdl run TARGET -- COMMAND` |
| `fdl.sync(target, command)` | `fdl sync TARGET -- COMMAND` |
| `fdl.connect(target)` | (use directly via DuckDB) |

`fdl.connect()` is the Python-only entry point — the CLI uses it internally
to implement `fdl sql`.

## Conventions

- `target` is a **required positional argument**. There is no implicit
  `"default"` target; pass the target name explicitly.
- Each function accepts a `project_dir: Path | None` keyword. When omitted,
  fdl walks up from the current working directory to find the nearest
  `fdl.toml`, mirroring CLI behavior.
- Console output (progress lines, conflict detection, etc.) matches the CLI
  and is written to stdout.
- `fdl.run()` and `fdl.sync()` return the subprocess exit code as an `int`.
  They do not raise on non-zero exit; check the return value.
- `fdl.init()` is **not idempotent**: it raises `FileExistsError` if
  `fdl.toml` is already present. Initialize once (typically via the CLI)
  and commit `fdl.toml` to the repo; the Python API is for day-to-day
  operations, not reinitialization.

See [Dagster](../integrations/dagster.md) for a worked example of using
these APIs inside a Dagster asset.

## Reference

::: fdl
    options:
      members:
        - init
        - pull
        - push
        - run
        - sync
        - connect
        - default_target_url
        - fdl_target_dir
        - ducklake_data_path
