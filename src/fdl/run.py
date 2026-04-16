"""Run: execute a command with fdl environment variables."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from fdl.console import console


def run_command(
    target: str,
    cmd: list[str],
    project_dir: Path | None = None,
) -> int:
    """Run a command with fdl environment variables.

    Performs auto-pull if needed, sets up env vars, runs subprocess.
    Returns the subprocess exit code. The subprocess inherits ``project_dir``
    as its working directory so that pipeline tools resolve paths relative to
    the project root.
    """
    from fdl import fdl_target_dir
    from fdl.config import (
        catalog_type,
        datasource_name,
        fdl_env_dict,
        find_project_dir,
        resolve_target,
        target_public_url,
        target_storage_url,
    )
    from fdl.ducklake import init_ducklake

    root = project_dir or find_project_dir()
    resolved = resolve_target(target, root)
    datasource = datasource_name(root)
    storage_val = target_storage_url(target, root)

    target_dir = root / fdl_target_dir(target)
    target_dir.mkdir(parents=True, exist_ok=True)

    # Auto-pull if local catalog is missing, unsynced, or stale
    from fdl.pull import pull_if_needed

    reason = pull_if_needed(target_dir, resolved, target, datasource, root)
    if reason:
        console.print(f"[dim]{reason}, pulled from {target}[/dim]")

    # Ensure target catalog exists (initialize on first run)
    pub = target_public_url(target, root) or "http://localhost:4001"
    init_ducklake(
        target_dir, root, public_url=pub, sqlite=catalog_type(root) == "sqlite"
    )

    # Build env with all FDL_* values (won't override existing env vars)
    env = os.environ.copy()
    for key, value in fdl_env_dict(
        target_name=target, storage_override=storage_val, project_dir=root,
    ).items():
        if key not in env:
            env[key] = value
    env.setdefault("PYTHONUNBUFFERED", "1")

    result = subprocess.run(cmd, env=env, cwd=root)
    return result.returncode
