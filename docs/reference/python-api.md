# Python API Reference

fdl exposes the same operations available in the [CLI](cli.md) as a Python
package, so pipelines and orchestrators (e.g. Dagster, Airflow) can drive
DuckLake catalogs without spawning a subprocess.

## At a glance

```python
import fdl

fdl.init("mydata", publish_url="s3://public-bucket/mydata")

with fdl.connect() as conn:
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")

fdl.publish()
```

## Function-to-command mapping

| Python API | CLI |
|---|---|
| `fdl.init(name, ...)` | `fdl init NAME` |
| `fdl.clone(url)` | `fdl clone URL` |
| `fdl.publish(name=None)` | `fdl publish [NAME]` |
| `fdl.run(publish_name, command)` | `fdl run [NAME] -- COMMAND` |
| `fdl.connect()` | `fdl sql` / `fdl duckdb` |

## Conventions

- `fdl.connect()` and `fdl.publish()` take no required positional argument: the live catalog is always resolved from `[metadata]`/`[data]` in `fdl.toml`.
- Each function accepts a `project_dir: Path | None` keyword. When omitted, fdl walks up from the current working directory to find the nearest `fdl.toml`, mirroring CLI behavior.
- `fdl.run()` returns the subprocess exit code as an `int`. It does not raise on non-zero exit; check the return value. On success, `fdl.run()` invokes publish automatically (see below).
- `fdl.publish(name=None)`:
  - `None` with exactly one `[publishes.*]` → implicit
  - `None` with multiple entries → raises `ValueError`
  - `None` with zero entries → raises `KeyError`
- `fdl.init()` is **not idempotent**: it raises `FileExistsError` if `fdl.toml` is already present.

See [Dagster](../integrations/dagster.md) for a worked example of using these APIs inside a Dagster asset.

## Reference

::: fdl
    options:
      members:
        - init
        - clone
        - publish
        - run
        - connect
