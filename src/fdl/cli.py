"""fdl CLI entry point."""

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
    force: bool = typer.Option(False, "--force", "-f", help="Force re-download even if up to date"),
) -> None:
    """Pull DuckLake catalog from a target."""
    from fdl import fdl_target_dir
    from fdl.config import datasource_name
    from fdl.pull import do_pull, pull_if_needed

    dataset_dir = Path.cwd()
    dist_dir = dataset_dir / fdl_target_dir(source)
    datasource = datasource_name(dataset_dir)
    resolved = _resolve_target(source)
    console.print(f"[bold]--- pull: {datasource} ← {resolved} ---[/bold]")

    if force:
        do_pull(resolved, source, dist_dir, datasource)
    else:
        reason = pull_if_needed(dist_dir, resolved, source, datasource)
        if reason:
            console.print(f"  {reason}")
        else:
            console.print("  Already up to date")


@app.command()
def push(
    dest: str = typer.Argument(..., help="Target name (e.g. default)"),
    force: bool = typer.Option(False, "--force", "-f", help="Override conflict detection"),
) -> None:
    """Push catalog to a target."""
    from fdl.push import do_push

    do_push(dest, force=force)


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def run(ctx: typer.Context) -> None:
    """Run a command with fdl environment variables.

    \b
    Sets FDL_STORAGE, FDL_DATA_PATH, FDL_CATALOG, FDL_S3_*:
      fdl run TARGET
      fdl run TARGET -- COMMAND [ARGS...]
    """
    from fdl.run import run_command

    target, cmd = _parse_command_args(ctx.args)

    raise SystemExit(run_command(target, cmd))


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def sync(
    ctx: typer.Context,
    force: bool = typer.Option(False, "--force", "-f", help="Override conflict detection on push"),
) -> None:
    """Run pipeline and push in one step.

    \b
    Uses command from fdl.toml, or specify explicitly:
      fdl sync TARGET
      fdl sync TARGET -- COMMAND [ARGS...]
    """
    from fdl.run import run_command

    target, cmd = _parse_command_args(ctx.args)

    returncode = run_command(target, cmd)

    if returncode != 0:
        console.print(f"[yellow]Command exited with code {returncode}, skipping push[/yellow]")
        raise SystemExit(returncode)

    from fdl.push import do_push

    do_push(target, force=force)


def _parse_command_args(args: list[str]) -> tuple[str, list[str]]:
    """Parse TARGET [-- COMMAND] from raw args, falling back to fdl.toml command."""
    import shlex

    if "--" in args:
        idx = args.index("--")
        before, cmd = args[:idx], args[idx + 1 :]
        if len(before) != 1:
            raise typer.BadParameter("Usage: fdl <command> TARGET [-- COMMAND]")
        if not cmd:
            raise typer.BadParameter("No command after --")
        return before[0], cmd

    # No -- separator: use command from fdl.toml
    if len(args) != 1:
        raise typer.BadParameter("Usage: fdl <command> TARGET [-- COMMAND]")
    target = args[0]

    from fdl.config import target_command

    command_str = target_command(target)
    if not command_str:
        raise typer.BadParameter(
            "No command specified and no command set in fdl.toml.\n"
            "  Either use: fdl <command> TARGET -- COMMAND\n"
            "  Or add to fdl.toml:\n"
            '    command = "python main.py"'
        )
    return target, shlex.split(command_str)


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
