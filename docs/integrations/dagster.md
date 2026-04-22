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
touching the same catalog, wrap `fdl.pull` / `fdl.connect` / `fdl.push` in a
[Dagster ConfigurableResource](https://docs.dagster.io/guides/operate/resources)
so the resource handles publishing and the asset only describes the
transformation:

```python
from contextlib import contextmanager
from pathlib import Path

import fdl
from dagster import ConfigurableResource, Definitions, EnvVar, asset


class FDLResource(ConfigurableResource):
    """Manages an fdl DuckLake catalog for Dagster assets.

    ``get_connection`` pulls before opening the catalog and, when
    ``read_only=False``, pushes on successful exit. Push errors propagate
    so a failed publish surfaces as an asset failure rather than silently
    leaving the remote catalog out of date.
    """

    target: str = "default"
    project_dir: str | None = None

    def _project_dir(self) -> Path | None:
        return Path(self.project_dir) if self.project_dir else None

    def pull(self, *, force: bool = False) -> None:
        fdl.pull(self.target, force=force, project_dir=self._project_dir())

    def push(self, *, force: bool = False) -> None:
        fdl.push(self.target, force=force, project_dir=self._project_dir())

    @contextmanager
    def get_connection(self, *, read_only: bool = False):
        self.pull()
        with fdl.connect(self.target, project_dir=self._project_dir()) as conn:
            yield conn
        if not read_only:
            self.push()


defs = Definitions(
    resources={
        "fdl": FDLResource(target=EnvVar("FDL_TARGET")),
    },
)


@asset
def cities(fdl: FDLResource) -> None:
    with fdl.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE cities AS SELECT 'Tokyo' AS name")


@asset
def city_count(fdl: FDLResource) -> int:
    with fdl.get_connection(read_only=True) as conn:
        return conn.execute("SELECT count(*) FROM cities").fetchone()[0]
```

`get_connection` calls `fdl.push` only after the `with` block exits
normally, so an exception inside the asset body leaves the remote catalog
untouched — matching the Dagster convention that a failed asset has not
produced output. Pass `read_only=True` for assets that only query the
catalog to skip the push.

`EnvVar("FDL_TARGET")` lets the same code point at different targets per
environment (e.g. `dev` locally, `prod` in the deployment). Leave
`project_dir` unset when `fdl.toml` is reachable by walking up from the
Dagster process's working directory; set it when the working directory is
not predictable.

## Avoiding push conflicts with Dagster pools

`fdl.push` to an S3 target uses an `If-Match` ETag precondition. When two
assets push to the same S3 catalog in parallel, the second push raises
`fdl.meta.PushConflictError`. Local file targets skip this check.

fdl has no built-in retry or merge. Serialize catalog writes at the
Dagster level with a concurrency pool in `dagster.yaml`:

```yaml
concurrency:
  pools:
    default_limit: 1
```

For finer control (named pools, per-pool limits), see the
[Dagster pools docs](https://docs.dagster.io/guides/operate/managing-concurrency#pools).

## Error handling

| Error class | Raised when |
|---|---|
| `FileNotFoundError` | `fdl.toml` is not found above the working directory (and no `project_dir` was provided) |
| `FileExistsError` | `fdl.init` called when `fdl.toml` already exists (init is not idempotent) |
| `ValueError` | Target not defined in `fdl.toml`, or `fdl.run` invoked without a `command` and no `command` in `fdl.toml` |
| `fdl.meta.PushConflictError` | `fdl.push` to an S3 target detected that the remote was updated since the last pull. Also surfaces when parallel assets push to the same target — see [Avoiding push conflicts with Dagster pools](#avoiding-push-conflicts-with-dagster-pools). Pass `force=True` to override. |

## Tip: initialize outside Dagster

`fdl.init` is not idempotent, so don't call it from an asset that may rerun.
Run `fdl init` once from the CLI, commit `fdl.toml` to your repo, and have
Dagster assets call only `fdl.pull`, `fdl.connect`, `fdl.push`, etc.
