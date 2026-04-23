"""Run: execute a command, then publish on success."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from fdl.console import console


def _resolve_command(
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


def run_command(
    publish_name: str | None,
    cmd: list[str] | None,
    *,
    project_dir: Path | None = None,
) -> int:
    """Run a pipeline command, then publish if any publish target is defined.

    - Resolves ``cmd`` from fdl.toml when ``None``.
    - Sets FDL_* env vars from [metadata] / [data].
    - Runs the subprocess with ``project_dir`` as cwd.
    - On non-zero exit, skips publish and returns the exit code unchanged.
    - On success, publishes: explicit ``publish_name`` wins; otherwise,
      if exactly one ``[publishes.*]`` is defined, it is used implicitly;
      with none defined, publish is skipped; with several, raise.
    """
    from fdl.config import (
        fdl_env_dict,
        find_project_dir,
        metadata_spec,
        publish_names,
    )

    root = project_dir or find_project_dir()

    # Resolve the command.
    if cmd is None:
        cmd_str = _resolve_command(publish_name, root)
        if not cmd_str:
            raise ValueError(
                "No command provided and no 'command' set in fdl.toml"
            )
        cmd = shlex.split(cmd_str)

    # Fail fast when a file-based live catalog is missing.
    spec = metadata_spec(root)
    if spec.scheme in ("sqlite", "duckdb") and spec.path:
        if not Path(spec.path).exists():
            raise FileNotFoundError(
                f"{spec.path} not found. Run 'fdl init' or 'fdl clone' first."
            )

    env = os.environ.copy()
    for key, value in fdl_env_dict(
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
