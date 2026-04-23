"""Run: execute a command with fdl environment variables."""

from __future__ import annotations

import os
import shlex
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
    from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, fdl_target_dir
    from fdl.config import (
        datasource_name,
        fdl_env_dict,
        find_project_dir,
        resolve_target,
        target_storage_url,
    )

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

    if not (
        (target_dir / DUCKLAKE_SQLITE).exists()
        or (target_dir / DUCKLAKE_FILE).exists()
    ):
        raise FileNotFoundError(
            f"No catalog for target '{target}'. "
            f"Run 'fdl init' or 'fdl pull {target}' first."
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


# ---------------------------------------------------------------------------
# v0.11: run = execute + implicit publish (replaces sync).
# ---------------------------------------------------------------------------


def _resolve_command_v11(
    publish_name: str | None,
    project_dir: Path,
) -> str | None:
    """Look up the pipeline command from fdl.toml.

    Precedence: ``publishes.<name>.command`` → top-level ``command``.
    """
    from fdl.config import PROJECT_CONFIG, _load_toml

    data = _load_toml(project_dir / PROJECT_CONFIG)
    if publish_name:
        pub = data.get("publishes", {}).get(publish_name)
        if isinstance(pub, dict) and pub.get("command"):
            return pub["command"]
    return data.get("command")


def run_command_v11(
    publish_name: str | None,
    cmd: list[str] | None,
    *,
    project_dir: Path | None = None,
) -> int:
    """Run a pipeline command, then publish if any publish target is defined.

    - Resolves ``cmd`` from fdl.toml when ``None``.
    - Sets FDL_* env vars from the v0.11 [metadata]/[data] schema.
    - Runs the subprocess with ``project_dir`` as cwd.
    - On non-zero exit, skips publish and propagates the exit code.
    - On success, publishes: explicit ``publish_name`` wins; otherwise,
      if exactly one [publishes.*] is defined, it's used implicitly; if
      none are defined, the publish step is skipped; if several are
      defined, fail with a clear error.
    """
    from fdl.config import (
        fdl_env_dict_v11,
        find_project_dir,
        metadata_spec,
        publish_names,
    )

    root = project_dir or find_project_dir()

    # Resolve the command.
    if cmd is None:
        cmd_str = _resolve_command_v11(publish_name, root)
        if not cmd_str:
            raise ValueError(
                "No command provided and no 'command' set in fdl.toml"
            )
        cmd = shlex.split(cmd_str)

    # Verify a live catalog is reachable before running (fail fast).
    spec = metadata_spec(root)
    if spec.scheme in ("sqlite", "duckdb") and spec.path:
        if not Path(spec.path).exists():
            raise FileNotFoundError(
                f"{spec.path} not found. Run 'fdl init' or 'fdl clone' first."
            )
    # Postgres connectivity is validated by the subprocess (lazy).

    env = os.environ.copy()
    for key, value in fdl_env_dict_v11(
        publish_name=publish_name, project_dir=root
    ).items():
        if key not in env:
            env[key] = value
    env.setdefault("PYTHONUNBUFFERED", "1")

    result = subprocess.run(cmd, env=env, cwd=root)
    if result.returncode != 0:
        console.print(
            f"[yellow]Command exited with code {result.returncode}, "
            f"skipping publish[/yellow]"
        )
        return result.returncode

    # Implicit publish step.
    names = publish_names(root)
    target: str | None
    if publish_name is not None:
        target = publish_name
    elif len(names) == 1:
        target = names[0]
    elif len(names) == 0:
        target = None
    else:
        raise ValueError(
            f"Multiple [publishes.*] defined ({names}); specify one explicitly."
        )
    if target is not None:
        from fdl.publish import publish

        publish(target, project_dir=root)
    return 0
