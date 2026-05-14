"""Frozen DuckLake: manage DuckLake catalogs on object storage.

fdl is both a CLI (``fdl``) and a Python API. The CLI handlers are thin
wrappers over the same internal functions exposed as the Python API, so the
two stay in sync by construction.

Example:
    >>> import fdl
    >>> fdl.init("my_ds")
    >>> with fdl.connect() as conn:
    ...     conn.execute("CREATE TABLE cities (name VARCHAR, pop INTEGER)")
    >>> fdl.publish()
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb

FDL_DIR = Path(".fdl")
DUCKLAKE_FILE = "ducklake.duckdb"
DUCKLAKE_SQLITE = "ducklake.sqlite"
META_JSON = "meta.json"


# ---------------------------------------------------------------------------
# Public Python API — mirrors the CLI commands.
#
# Submodules are imported here at the bottom of the file rather than lazily
# inside each function so that the public function definitions below shadow
# the submodule attributes on the ``fdl`` package.
# ---------------------------------------------------------------------------

from fdl.clone import clone as _do_clone  # noqa: E402
from fdl.init_project import init_project as _do_init  # noqa: E402
from fdl.publish import publish as _do_publish  # noqa: E402
from fdl.run import run_command as _run_command  # noqa: E402


def init(
    name: str,
    *,
    metadata_url: str | None = None,
    data_url: str | None = None,
    publish_url: str | None = None,
    publish_name: str = "default",
    project_dir: Path | None = None,
) -> None:
    """Initialize a new fdl project (CLI: ``fdl init``).

    Writes a v0.11 ``fdl.toml`` and provisions the live catalog.
    """
    _do_init(
        name,
        metadata_url=metadata_url,
        data_url=data_url,
        publish_url=publish_url,
        publish_name=publish_name,
        project_dir=project_dir,
    )


def clone(
    url: str,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Clone a published frozen DuckLake into a new local live catalog."""
    _do_clone(url, project_dir=project_dir, force=force)


def publish(
    name: str | None = None,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Convert the live catalog to a frozen DuckDB and upload it."""
    _do_publish(name, project_dir=project_dir, force=force)


def run(
    publish_name: str | None = None,
    command: list[str] | None = None,
    *,
    project_dir: Path | None = None,
) -> int:
    """Run a pipeline and publish on success (CLI: ``fdl run``)."""
    return _run_command(publish_name, command, project_dir=project_dir)


@contextmanager
def connect(
    *,
    read_only: bool = False,
    project_dir: Path | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection with the live DuckLake catalog attached."""
    from fdl.ducklake import connect as _connect

    with _connect(read_only=read_only, project_dir=project_dir) as conn:
        yield conn


__all__ = [
    # Constants
    "DUCKLAKE_FILE",
    "DUCKLAKE_SQLITE",
    "FDL_DIR",
    "META_JSON",
    # Python API
    "clone",
    "connect",
    "init",
    "publish",
    "run",
]
