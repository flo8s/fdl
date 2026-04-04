"""Run: execute a command with fdl environment variables."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from fdl.console import console


def run_command(target: str, cmd: list[str]) -> int:
    """Run a command with fdl environment variables.

    Performs auto-pull if needed, sets up env vars, runs subprocess.
    Returns the subprocess exit code.
    """
    from fdl import fdl_target_dir
    from fdl.config import (
        catalog_type,
        datasource_name,
        fdl_env_dict,
        resolve_target,
        target_public_url,
    )
    from fdl.ducklake import init_ducklake

    resolved = resolve_target(target, Path.cwd())
    datasource = datasource_name()
    storage_val = f"{resolved}/{datasource}"

    target_dir = Path.cwd() / fdl_target_dir(target)
    target_dir.mkdir(parents=True, exist_ok=True)

    # Auto-pull if local catalog is missing, unsynced, or stale
    from fdl.pull import pull_if_needed

    reason = pull_if_needed(target_dir, resolved, target, datasource)
    if reason:
        console.print(f"[dim]{reason}, pulled from {target}[/dim]")

    # Ensure target catalog exists (initialize on first run)
    pub = target_public_url(target) or "http://localhost:4001"
    init_ducklake(
        target_dir, Path.cwd(), public_url=pub, sqlite=catalog_type() == "sqlite"
    )

    # Build env with all FDL_* values (won't override existing env vars)
    env = os.environ.copy()
    for key, value in fdl_env_dict(
        target_name=target, storage_override=storage_val
    ).items():
        if key not in env:
            env[key] = value
    env.setdefault("PYTHONUNBUFFERED", "1")

    result = subprocess.run(cmd, env=env)
    return result.returncode
