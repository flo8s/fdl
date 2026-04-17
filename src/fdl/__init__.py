"""Frozen DuckLake: manage DuckLake catalogs on object storage.

fdl is both a CLI (``fdl``) and a Python API. The CLI handlers are thin
wrappers over the same internal functions exposed as the Python API, so the
two stay in sync by construction.

Example:
    >>> import fdl
    >>> fdl.pull("default")
    >>> with fdl.connect("default") as conn:
    ...     conn.execute("CREATE TABLE cities (name VARCHAR, pop INTEGER)")
    >>> fdl.push("default")
"""

from __future__ import annotations

import os
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


def fdl_target_dir(target_name: str) -> Path:
    """Target-specific directory under .fdl/."""
    return FDL_DIR / target_name


def ducklake_data_path(catalog_url: str) -> str:
    """Derive DuckLake DATA_PATH from a catalog URL or path."""
    return f"{catalog_url}.files/"


def default_target_url() -> str:
    """Default target URL ($XDG_DATA_HOME/fdl or ~/.local/share/fdl).

    Returns a display-friendly path using ~ when under the home directory.
    """
    xdg = os.environ.get("XDG_DATA_HOME")
    result = Path(xdg, "fdl") if xdg else Path.home() / ".local" / "share" / "fdl"
    home = Path.home()
    try:
        return str(Path("~") / result.relative_to(home))
    except ValueError:
        return str(result)


# ---------------------------------------------------------------------------
# Public Python API — mirrors the CLI commands.
#
# Submodules are imported here at the bottom of the file rather than lazily
# inside each function so that the public function definitions below shadow
# the submodule attributes on the ``fdl`` package (Python sets
# ``fdl.push = <module>`` on import, which would otherwise hide the ``push``
# function defined below).
# ---------------------------------------------------------------------------

from fdl.push import do_push as _do_push  # noqa: E402
from fdl.pull import (  # noqa: E402
    do_pull as _do_pull,
    pull_if_needed as _pull_if_needed,
)
from fdl.run import run_command as _run_command  # noqa: E402


def init(
    name: str,
    *,
    target_name: str = "default",
    target_url: str | None = None,
    public_url: str = "http://localhost:4001",
    sqlite: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Initialize an fdl project (CLI: ``fdl init``).

    Writes ``fdl.toml`` and creates ``.fdl/{target_name}/ducklake.duckdb``.
    On failure, partially created ``fdl.toml`` / ``.fdl/`` are rolled back.

    Args:
        name: Datasource name. Must be a valid SQL identifier.
        target_name: Target name in fdl.toml.
        target_url: Storage URL for the target. Defaults to
            :func:`default_target_url`.
        public_url: Public URL the dataset will be served under.
        sqlite: Use SQLite catalog instead of DuckDB (for dlt compatibility).
        project_dir: Directory to initialize in. Defaults to ``Path.cwd()``.

    Raises:
        ValueError: If ``name`` is not a valid SQL identifier.
        FileExistsError: If ``fdl.toml`` already exists in the project.
    """
    import re
    import shutil

    from fdl.config import PROJECT_CONFIG, set_value
    from fdl.ducklake import init_ducklake

    root = project_dir or Path.cwd()

    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    if sanitized != name:
        raise ValueError(
            f"'{name}' is not a valid SQL identifier. Use '{sanitized}' instead."
        )

    config_path = root / PROJECT_CONFIG
    if config_path.exists():
        raise FileExistsError(f"{config_path} already exists")

    if target_url is None:
        target_url = default_target_url()

    dist_dir = root / fdl_target_dir(target_name)
    try:
        set_value("name", name, config_path)
        set_value("catalog", "sqlite" if sqlite else "duckdb", config_path)
        set_value(f"targets.{target_name}.url", target_url, config_path)
        set_value(f"targets.{target_name}.public_url", public_url, config_path)
        init_ducklake(dist_dir, root, public_url=public_url, sqlite=sqlite)
    except Exception:
        if config_path.exists():
            config_path.unlink()
        if dist_dir.exists():
            shutil.rmtree(dist_dir)
        fdl_dir = root / FDL_DIR
        if fdl_dir.exists() and not any(fdl_dir.iterdir()):
            fdl_dir.rmdir()
        raise


def pull(
    target: str,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Pull DuckLake catalog from a target (CLI: ``fdl pull``).

    Args:
        target: Target name defined in fdl.toml.
        force: Re-download even if local catalog is up to date.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Raises:
        FileNotFoundError: If ``fdl.toml`` cannot be located.
        ValueError: If ``target`` is not defined in ``fdl.toml``.
    """
    from fdl.config import datasource_name, find_project_dir, resolve_target
    from fdl.console import console

    root = project_dir or find_project_dir()
    dist_dir = root / fdl_target_dir(target)
    datasource = datasource_name(root)
    resolved = resolve_target(target, root)
    console.print(f"[bold]--- pull: {datasource} ← {resolved} ---[/bold]")

    if force:
        _do_pull(resolved, target, dist_dir, datasource, root)
    else:
        reason = _pull_if_needed(dist_dir, resolved, target, datasource, root)
        if reason:
            console.print(f"  {reason}")
        else:
            console.print("  Already up to date")


def push(
    target: str,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> None:
    """Push catalog to a target (CLI: ``fdl push``).

    Args:
        target: Target name defined in fdl.toml.
        force: Override conflict detection.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Raises:
        FileNotFoundError: If ``fdl.toml`` cannot be located.
        ValueError: If ``target`` is not defined in ``fdl.toml``.
        fdl.meta.PushConflictError: When the remote has been updated since
            the last pull (only when ``force=False``).
    """
    _do_push(target, force=force, project_dir=project_dir)


def run(
    target: str,
    command: list[str] | None = None,
    *,
    project_dir: Path | None = None,
) -> int:
    """Run ``command`` with fdl environment variables (CLI: ``fdl run``).

    Auto-pulls the catalog if stale, sets ``FDL_STORAGE`` / ``FDL_DATA_PATH``
    / ``FDL_CATALOG`` / ``FDL_S3_*`` in the subprocess environment, and runs
    ``command`` with ``project_dir`` as its working directory (so relative
    paths resolve against the project root, not the caller's cwd).

    Args:
        target: Target name defined in fdl.toml.
        command: Command and arguments to run. When ``None``, reads the
            ``command`` field from fdl.toml
            (``targets.<target>.command`` or top-level ``command``).
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Returns:
        The subprocess exit code.

    Raises:
        FileNotFoundError: If ``fdl.toml`` cannot be located.
        ValueError: If ``target`` is not defined in ``fdl.toml``, or if
            ``command`` is ``None`` and no ``command`` is set in ``fdl.toml``.
    """
    import shlex

    from fdl.config import find_project_dir, target_command

    root = project_dir or find_project_dir()
    if command is None:
        cmd_str = target_command(target, root)
        if not cmd_str:
            raise ValueError(
                "No command provided and no 'command' set in fdl.toml"
            )
        command = shlex.split(cmd_str)
    return _run_command(target, command, root)


def sync(
    target: str,
    command: list[str] | None = None,
    *,
    force: bool = False,
    project_dir: Path | None = None,
) -> int:
    """Run command then push in one step (CLI: ``fdl sync``).

    When the command exits non-zero, push is skipped and the exit code is
    returned as-is.

    Args:
        target: Target name defined in fdl.toml.
        command: Command to run. When ``None``, reads the ``command`` field
            from fdl.toml.
        force: Override conflict detection on push.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Returns:
        The subprocess exit code (``0`` if both run and push succeeded).

    Raises:
        FileNotFoundError: If ``fdl.toml`` cannot be located.
        ValueError: If ``target`` is not defined in ``fdl.toml``, or if
            ``command`` is ``None`` and no ``command`` is set in ``fdl.toml``.
        fdl.meta.PushConflictError: When the remote has been updated since
            the last pull (only when ``force=False``).
    """
    from fdl.console import console

    returncode = run(target, command, project_dir=project_dir)
    if returncode != 0:
        console.print(
            f"[yellow]Command exited with code {returncode}, skipping push[/yellow]"
        )
        return returncode
    push(target, force=force, project_dir=project_dir)
    return 0


@contextmanager
def connect(
    target: str,
    *,
    project_dir: Path | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection with the DuckLake catalog attached.

    The datasource (from fdl.toml ``name``) is attached and selected via
    ``USE``, so table references can be bare::

        with fdl.connect("default") as conn:
            conn.execute("CREATE TABLE cities (...)")
            rows = conn.execute("SELECT * FROM cities").fetchall()

    Args:
        target: Target name defined in fdl.toml.
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.

    Yields:
        A DuckDB connection with the DuckLake catalog attached.

    Raises:
        FileNotFoundError: If ``fdl.toml`` or the local catalog file cannot
            be located. Run ``fdl.init`` or ``fdl.pull`` first.
        ValueError: If ``target`` is not defined in ``fdl.toml``.
    """
    from fdl.config import find_project_dir
    from fdl.ducklake import connect as _connect

    root = project_dir or find_project_dir()

    with _connect(target_name=target, project_dir=root) as conn:
        yield conn


__all__ = [
    # Constants
    "DUCKLAKE_FILE",
    "DUCKLAKE_SQLITE",
    "FDL_DIR",
    "META_JSON",
    # Helpers
    "default_target_url",
    "ducklake_data_path",
    "fdl_target_dir",
    # Python API
    "connect",
    "init",
    "pull",
    "push",
    "run",
    "sync",
]
