# Dagster

fdl's [Python API](../reference/python-api.md) can be called directly from
Dagster assets and resources. Unlike spawning `fdl` as a subprocess, this
keeps catalog connections in-process, lets Dagster surface exceptions, and
avoids shell-escaping headaches around SQL.

## Minimal example

```python
from pathlib import Path

import fdl
from dagster import asset

PROJECT = Path("/abs/path/to/fdl-project")


@asset
def world_cities() -> None:
    """Materialize world_cities into the DuckLake catalog."""
    fdl.pull("default", project_dir=PROJECT)

    with fdl.connect("default", project_dir=PROJECT) as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE world_cities AS "
            "SELECT * FROM read_csv_auto('world_cities.csv')"
        )

    fdl.push("default", project_dir=PROJECT)
```

Passing `project_dir` explicitly avoids any dependency on the Dagster
process's working directory. When the Dagster project and the fdl project
share a root (and `fdl.toml` is reachable by walking up from cwd), the
argument can be omitted.

## Sharing a connection via Resource

Opening a connection per asset is fine for small pipelines. For many assets
touching the same catalog, wrap `fdl.connect` in a
[Dagster ConfigurableResource](https://docs.dagster.io/guides/operate/resources):

```python
from contextlib import contextmanager
from pathlib import Path

import fdl
from dagster import ConfigurableResource, asset


class FdlCatalog(ConfigurableResource):
    project_dir: str
    target: str = "default"

    @contextmanager
    def connect(self):
        with fdl.connect(self.target, project_dir=Path(self.project_dir)) as conn:
            yield conn


@asset
def cities(fdl_catalog: FdlCatalog) -> None:
    fdl.pull(fdl_catalog.target, project_dir=Path(fdl_catalog.project_dir))
    with fdl_catalog.connect() as conn:
        conn.execute("CREATE OR REPLACE TABLE cities AS SELECT 'Tokyo' AS name")
    fdl.push(fdl_catalog.target, project_dir=Path(fdl_catalog.project_dir))
```

## Running an existing pipeline script

If you already drive a pipeline with `fdl run` (for example a dbt or dlt
project), call `fdl.run` from an asset and propagate the exit code:

```python
from pathlib import Path

import fdl
from dagster import Failure, asset

PROJECT = Path("/abs/path/to/fdl-project")


@asset
def pipeline() -> None:
    rc = fdl.run("default", ["python", "main.py"], project_dir=PROJECT)
    if rc != 0:
        raise Failure(f"pipeline exited with code {rc}")
```

`fdl.sync` combines `fdl.run` + `fdl.push` and is similarly suited to a
single-asset pipeline that also needs to deploy on success.

## Error handling

| Error class | Raised when |
|---|---|
| `FileNotFoundError` | `fdl.toml` is not found above the working directory (and no `project_dir` was provided) |
| `FileExistsError` | `fdl.init` called when `fdl.toml` already exists (init is not idempotent) |
| `ValueError` | Target not defined in `fdl.toml`, or `fdl.run` invoked without a `command` and no `command` in `fdl.toml` |
| `fdl.meta.PushConflictError` | `fdl.push` detected that the remote was updated since the last pull (pass `force=True` to override) |

## Tip: initialize outside Dagster

`fdl.init` is not idempotent, so don't call it from an asset that may rerun.
Run `fdl init` once from the CLI, commit `fdl.toml` to your repo, and have
Dagster assets call only `fdl.pull`, `fdl.connect`, `fdl.push`, etc.
