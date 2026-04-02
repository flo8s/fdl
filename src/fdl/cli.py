"""fdl CLI entry point."""

import os
import re
from pathlib import Path

import typer
from typer.main import Typer

from fdl import FDL_DIR  # noqa: F401
from fdl.console import console

app = typer.Typer(pretty_exceptions_short=True, invoke_without_command=True)


def _sanitize_name(name: str) -> str:
    """Sanitize a directory name into a valid SQL identifier."""
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if name and name[0].isdigit():
        name = f"_{name}"
    return name


def _resolve_target(name: str) -> str:
    """Resolve a target name, converting ValueError to BadParameter."""
    from fdl.config import resolve_target

    try:
        return resolve_target(name, Path.cwd())
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None


def _version_callback(value: bool) -> None:
    if value:
        from importlib.metadata import version

        console.print(f"fdl {version('frozen-ducklake')}", highlight=False)
        raise typer.Exit()


@app.callback()
def callback(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit",
    ),
) -> None:
    """fdl: DuckLake catalog management CLI"""
    import click

    ctx = click.get_current_context()
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@app.command()
def init(
    name: str = typer.Argument(
        None, help="Datasource name (default: current directory name)"
    ),
    public_url: str = typer.Option(
        None, "--public-url", help="Public URL for dataset access"
    ),
    target_url: str = typer.Option(
        None, "--target-url", help="Target URL for push/pull"
    ),
    target_name: str = typer.Option(None, "--target-name", help="Target name"),
    sqlite: bool = typer.Option(
        False, help="Use SQLite catalog (for dlt compatibility)"
    ),
) -> None:
    """Initialize a new fdl project."""
    import shutil

    from fdl import default_target_url
    from fdl.config import PROJECT_CONFIG, set_value
    from fdl.ducklake import init_ducklake

    dataset_dir = Path.cwd()
    if name:
        sanitized = _sanitize_name(name)
        if sanitized != name:
            raise typer.BadParameter(
                f"'{name}' is not a valid SQL identifier. Use '{sanitized}' instead."
            )
    else:
        default_name = _sanitize_name(dataset_dir.resolve().name)
        name = typer.prompt("Datasource name", default=default_name)
    config_path = dataset_dir / PROJECT_CONFIG

    if config_path.exists():
        raise typer.BadParameter(f"{PROJECT_CONFIG} already exists")

    # Prompt for target config if not provided via flags
    if target_name is None:
        target_name = typer.prompt("Target name", default="default")
    if public_url is None:
        public_url = typer.prompt("Public URL", default="http://localhost:4001")
    if target_url is None:
        target_url = typer.prompt("Target URL", default=default_target_url())

    from fdl import fdl_target_dir

    dist_dir = dataset_dir / fdl_target_dir(target_name)
    try:
        # fdl.toml
        set_value("name", name, config_path)
        set_value("catalog", "sqlite" if sqlite else "duckdb", config_path)
        set_value(f"targets.{target_name}.url", target_url, config_path)
        set_value(f"targets.{target_name}.public_url", public_url, config_path)

        # .fdl/{target}/ + DuckLake catalog
        init_ducklake(dist_dir, dataset_dir, public_url=public_url, sqlite=sqlite)

    except Exception:
        # Rollback: remove partially created files
        if config_path.exists():
            config_path.unlink()
        if dist_dir.exists():
            shutil.rmtree(dist_dir)
        fdl_dir = dataset_dir / FDL_DIR
        if fdl_dir.exists() and not any(fdl_dir.iterdir()):
            fdl_dir.rmdir()
        raise

    console.print(f"[green]Initialized fdl project: {name}[/green]")


@app.command()
def pull(
    source: str = typer.Argument(..., help="Target name (e.g. default)"),
) -> None:
    """Pull DuckLake catalog from a target."""
    from fdl import fdl_target_dir
    from fdl.config import datasource_name

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / fdl_target_dir(source)
    datasource = datasource_name(dataset_dir)
    resolved = _resolve_target(source)
    console.print(f"[bold]--- pull: {datasource} ← {resolved} ---[/bold]")

    if resolved.startswith("s3://"):
        from fdl.config import target_s3_config
        from fdl.pull import fetch_from_s3
        from fdl.s3 import create_s3_client

        s3 = target_s3_config(source)
        client = create_s3_client(s3)
        fetch_from_s3(client, s3.bucket, dist_dir, datasource, target_name=source)
    else:
        from fdl.pull import pull_from_local

        pull_from_local(Path(resolved), dist_dir, datasource, target_name=source)


@app.command()
def push(
    dest: str = typer.Argument(..., help="Target name (e.g. default)"),
    force: bool = typer.Option(False, "--force", "-f", help="Override conflict detection"),
) -> None:
    """Push catalog to a target."""
    from fdl import fdl_target_dir
    from fdl.config import datasource_name
    from fdl.ducklake import convert_sqlite_to_duckdb
    from fdl.meta import PushConflictError

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / fdl_target_dir(dest)
    datasource = datasource_name(dataset_dir)

    resolved = _resolve_target(dest)
    console.print(f"[bold]--- push: {datasource} → {resolved} ---[/bold]")
    convert_sqlite_to_duckdb(dataset_dir, dest)

    try:
        if resolved.startswith("s3://"):
            from fdl.config import target_s3_config
            from fdl.push import push_to_s3
            from fdl.s3 import create_s3_client

            s3 = target_s3_config(dest)
            client = create_s3_client(s3)
            push_to_s3(client, s3.bucket, dist_dir, datasource, dataset_dir, force=force, target_name=dest)
        else:
            from fdl.push import push_to_local

            push_to_local(Path(resolved), dist_dir, datasource, dataset_dir, force=force, target_name=dest)
    except PushConflictError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def run(ctx: typer.Context) -> None:
    """Run a command with fdl environment variables.

    \b
    Sets FDL_STORAGE, FDL_DATA_PATH, FDL_CATALOG, FDL_S3_*:
      fdl run TARGET -- COMMAND [ARGS...]
    """
    import subprocess

    from fdl.config import datasource_name, fdl_env_dict

    target, cmd = _parse_run_args(ctx.args)

    if not cmd:
        raise typer.BadParameter(
            "No command specified. Usage: fdl run TARGET -- COMMAND"
        )

    from fdl import fdl_target_dir

    resolved = _resolve_target(target)
    storage_val = f"{resolved}/{datasource_name()}"

    # Ensure target catalog exists (initialize on first run)
    from fdl.config import target_public_url
    from fdl.ducklake import init_ducklake

    target_dir = Path.cwd() / fdl_target_dir(target)
    target_dir.mkdir(parents=True, exist_ok=True)
    pub = target_public_url(target) or "http://localhost:4001"
    init_ducklake(target_dir, Path.cwd(), public_url=pub)

    # Build env with all FDL_* values (won't override existing env vars)
    env = os.environ.copy()
    for key, value in fdl_env_dict(target_name=target, storage_override=storage_val).items():
        if key not in env:
            env[key] = value

    result = subprocess.run(cmd, env=env)
    raise SystemExit(result.returncode)


def _parse_run_args(args: list[str]) -> tuple[str, list[str]]:
    """Parse TARGET -- COMMAND from raw args."""
    if "--" not in args:
        raise typer.BadParameter("Usage: fdl run TARGET -- COMMAND")

    idx = args.index("--")
    before, cmd = args[:idx], args[idx + 1 :]

    if len(before) != 1:
        raise typer.BadParameter("Usage: fdl run TARGET -- COMMAND")
    return before[0], cmd


@app.command("config")
def config_cmd(
    key: str = typer.Argument(None, help="Config key (e.g. 'targets.default.url')"),
    value: str = typer.Argument(None, help="Value to set"),
) -> None:
    """Get or set fdl configuration (reads/writes fdl.toml)."""
    from fdl.config import (
        get_all,
        _load_toml,
        project_config_path,
        set_value,
    )

    dest = project_config_path()

    if key is None:
        for k, v in get_all(dest).items():
            console.print(f"{k}={v}", highlight=False)
        return

    if value is not None:
        set_value(key, value, dest)
    else:
        data = _load_toml(dest)
        parts = key.split(".")
        result = data
        for part in parts:
            if isinstance(result, dict):
                result = result.get(part)
            else:
                result = None
                break
        if result is None:
            raise SystemExit(1)
        console.print(result, highlight=False)


@app.command()
def sql(
    target: str = typer.Argument(..., help="Target name (e.g. default)"),
    query: str = typer.Argument(..., help="SQL query to execute"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip stale catalog check"
    ),
) -> None:
    """Execute a SQL query against the DuckLake catalog."""
    from fdl.config import datasource_name
    from fdl.ducklake import connect
    from fdl.meta import PushConflictError, check_conflict, read_remote_pushed_at

    resolved = _resolve_target(target)
    datasource = datasource_name()

    try:
        check_conflict(
            read_remote_pushed_at(resolved, target, datasource), force=force, target_name=target,
        )
    except PushConflictError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    storage_val = f"{resolved}/{datasource}"

    with connect(storage=storage_val, target_name=target) as conn:
        conn.execute(f"USE {datasource}")
        conn.execute(query)
        if not conn.description:
            return
        columns = [desc[0] for desc in conn.description]
        rows = conn.fetchall()
        if not rows:
            return
        # Format as table
        col_widths = [len(c) for c in columns]
        for row in rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        header = " | ".join(c.ljust(w) for c, w in zip(columns, col_widths))
        separator = "-+-".join("-" * w for w in col_widths)
        print(header)
        print(separator)
        for row in rows:
            print(" | ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))


@app.command()
def serve(
    target: str = typer.Argument(..., help="Target name (e.g. default)"),
    port: int = typer.Option(4001, help="Port number"),
) -> None:
    """Serve a target directory over HTTP (CORS + Range support)."""
    from fdl.serve import run_server

    serve_dir = Path(_resolve_target(target))
    run_server(serve_dir, port)


main: Typer = app

if __name__ == "__main__":
    main()
