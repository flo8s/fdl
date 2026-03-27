"""fdl CLI entry point."""

import os
from pathlib import Path

import typer
from typer.main import Typer

from fdl import FDL_DIR

app = typer.Typer(pretty_exceptions_short=True)


def _resolve_remote(name: str) -> str:
    """Resolve a remote name, converting ValueError to BadParameter."""
    from fdl.config import resolve_remote

    try:
        return resolve_remote(name, Path.cwd())
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None


def _version_callback(value: bool) -> None:
    if value:
        from importlib.metadata import version

        print(f"fdl {version('frozen-ducklake')}")
        raise typer.Exit()


@app.callback()
def callback(
    version: bool = typer.Option(
        False, "--version", "-V", callback=_version_callback, is_eager=True,
        help="Show version and exit",
    ),
) -> None:
    """fdl: DuckLake catalog management CLI"""


@app.command()
def init(
    name: str = typer.Argument(..., help="Datasource name"),
    sqlite: bool = typer.Option(
        False, help="Use SQLite catalog (for dlt compatibility)"
    ),
) -> None:
    """Initialize a new fdl project (like git init)."""
    import shutil

    from fdl.config import PROJECT_CONFIG, public_url, set_value
    from fdl.ducklake import init_ducklake

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / FDL_DIR
    config_path = dataset_dir / PROJECT_CONFIG

    if config_path.exists():
        raise typer.BadParameter(f"{PROJECT_CONFIG} already exists")

    # Validate prerequisites
    try:
        public_url()
    except KeyError as e:
        raise typer.BadParameter(str(e)) from None

    try:
        # fdl.toml
        set_value("name", name, config_path)
        if sqlite:
            set_value("catalog", "sqlite", config_path)

        # .fdl/ + DuckLake catalog
        init_ducklake(dist_dir, dataset_dir, sqlite=sqlite)

    except Exception:
        # Rollback: remove partially created files
        if config_path.exists():
            config_path.unlink()
        if dist_dir.exists():
            shutil.rmtree(dist_dir)
        raise

    # Idempotent — no rollback needed
    gitignore = dataset_dir / ".gitignore"
    marker = ".fdl/"
    if gitignore.exists():
        content = gitignore.read_text()
        if marker not in content:
            gitignore.write_text(content.rstrip() + f"\n{marker}\n")
    else:
        gitignore.write_text(f"{marker}\n")

    print(f"Initialized fdl project: {name}")


@app.command()
def pull(
    source: str = typer.Argument(
        ..., help="Remote name (e.g. origin)"
    ),
) -> None:
    """Pull DuckLake catalog from source."""
    from fdl.config import datasource_name

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / FDL_DIR
    datasource = datasource_name(dataset_dir)
    resolved = _resolve_remote(source)
    print(f"--- pull: {datasource} ← {resolved} ---")

    if resolved.startswith("s3://"):
        from fdl.pull import fetch_from_s3
        from fdl.s3 import create_s3_client

        bucket = resolved.removeprefix("s3://")
        client = create_s3_client()
        fetch_from_s3(client, bucket, dist_dir, datasource)
    else:
        from fdl.pull import pull_from_local

        pull_from_local(Path(resolved), dist_dir, datasource)


@app.command()
def push(
    dest: str = typer.Argument(
        ..., help="Remote name (e.g. origin, local)"
    ),
) -> None:
    """Push build artifacts"""
    from fdl.config import datasource_name
    from fdl.ducklake import convert_sqlite_to_duckdb

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / FDL_DIR
    datasource = datasource_name(dataset_dir)

    resolved = _resolve_remote(dest)
    print(f"--- push: {datasource} → {resolved} ---")
    convert_sqlite_to_duckdb(dataset_dir)

    if resolved.startswith("s3://"):
        from fdl.push import push_to_s3
        from fdl.s3 import create_s3_client

        bucket = resolved.removeprefix("s3://")
        client = create_s3_client()
        push_to_s3(client, bucket, dist_dir, datasource)
    else:
        from fdl.push import push_to_local

        push_to_local(Path(resolved), dist_dir, datasource)


@app.command()
def metadata(
    target_dir: str = typer.Argument("target", help="dbt target directory path"),
) -> None:
    """Generate metadata.json from dbt artifacts"""
    from fdl.metadata import _copy_docs_to_dist, generate_metadata

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / FDL_DIR
    target_path = Path(target_dir)
    generate_metadata(dataset_dir, dist_dir, target_path)
    _copy_docs_to_dist(target_path, dist_dir)


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def run(ctx: typer.Context) -> None:
    """Run a command with fdl environment variables.

    \b
    Sets FDL_STORAGE, FDL_DATA_PATH, FDL_ATTACH_PATH, FDL_S3_*:
      fdl run -- COMMAND [ARGS...]           # local (.fdl)
      fdl run REMOTE -- COMMAND [ARGS...]    # remote storage
    """
    import subprocess

    from fdl.config import datasource_name, fdl_env_dict

    remote, cmd = _parse_run_args(ctx.args)

    if not cmd:
        raise typer.BadParameter(
            "No command specified. Usage: fdl run [REMOTE] -- COMMAND"
        )

    # Compute storage for remote target
    storage_val = None
    if remote:
        resolved = _resolve_remote(remote)
        storage_val = f"{resolved}/{datasource_name()}"

    # Build env with all FDL_* values (won't override existing env vars)
    env = os.environ.copy()
    for key, value in fdl_env_dict(storage_override=storage_val).items():
        if key not in env:
            env[key] = value

    result = subprocess.run(cmd, env=env)
    raise SystemExit(result.returncode)


def _parse_run_args(args: list[str]) -> tuple[str | None, list[str]]:
    """Parse [REMOTE] -- COMMAND from raw args.

    Without "--": all args are the command (local mode).
    With "--": at most one arg before it is the remote name.
    """
    if "--" not in args:
        return None, list(args)

    idx = args.index("--")
    before, cmd = args[:idx], args[idx + 1:]

    # At most one remote name before "--"
    if len(before) > 1:
        raise typer.BadParameter(
            f"Expected at most one remote name before --, got: {' '.join(before)}"
        )
    remote = before[0] if before else None
    return remote, cmd


@app.command()
def gc(
    remote: str = typer.Argument(..., help="Remote name (e.g. origin)"),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n", help="List orphaned files without deleting"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip confirmation prompt"
    ),
    older_than_days: int = typer.Option(
        None, "--older-than", help="Only target files older than N days"
    ),
) -> None:
    """Clean up orphaned data files on remote storage."""
    from fdl.gc import gc_datasource

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / FDL_DIR

    resolved = _resolve_remote(remote)
    if not resolved.startswith("s3://"):
        raise typer.BadParameter("gc only supports S3 remotes")

    bucket = resolved.removeprefix("s3://")
    gc_datasource(
        dataset_dir,
        dist_dir,
        bucket=bucket,
        force=force,
        dry_run=dry_run,
        older_than_days=older_than_days,
    )


@app.command("config")
def config_cmd(
    key: str = typer.Argument(None, help="Config key (e.g. 's3.endpoint')"),
    value: str = typer.Argument(None, help="Value to set"),
    _list: bool = typer.Option(False, "--list", "-l", help="List all settings"),
    local: bool = typer.Option(False, "--local", help="Use project config (fdl.toml)"),
) -> None:
    """Get or set fdl configuration."""
    from fdl.config import (
        get_all,
        load_toml,
        workspace_config_path,
        set_value,
        user_config_path,
    )

    target = workspace_config_path() if local else user_config_path()

    if _list:
        for k, v in get_all(target).items():
            print(f"{k}={v}")
        return

    if key is None:
        raise typer.BadParameter("Specify a key or use --list")

    if value is not None:
        set_value(key, value, target)
    else:
        data = load_toml(target)
        section, name = key.split(".", 1)
        result = data.get(section, {}).get(name)
        if result is None:
            raise SystemExit(1)
        print(result)


@app.command()
def serve(
    remote: str = typer.Argument(None, help="Remote name (omit for current project's .fdl/)"),
    port: int = typer.Option(4001, help="Port number"),
) -> None:
    """Serve over HTTP (CORS + Range support).

    \b
    Without remote: serves .fdl/ of current project (single dataset).
    With remote: serves the remote directory (multi-dataset).
    """
    from fdl.serve import run_server

    if remote:
        serve_dir = Path(_resolve_remote(remote))
    else:
        serve_dir = Path.cwd() / FDL_DIR
    run_server(serve_dir, port)


main: Typer = app

if __name__ == "__main__":
    main()
