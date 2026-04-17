"""fdl CLI entry point."""

import re
from pathlib import Path

import typer
from typer.main import Typer

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
        return resolve_target(name)
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
    import fdl
    from fdl.config import PROJECT_CONFIG

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

    if (dataset_dir / PROJECT_CONFIG).exists():
        raise typer.BadParameter(f"{PROJECT_CONFIG} already exists")

    # Prompt for target config if not provided via flags
    if target_name is None:
        target_name = typer.prompt("Target name", default="default")
    if public_url is None:
        public_url = typer.prompt("Public URL", default="http://localhost:4001")
    if target_url is None:
        target_url = typer.prompt("Target URL", default=fdl.default_target_url())

    fdl.init(
        name,
        target_name=target_name,
        target_url=target_url,
        public_url=public_url,
        sqlite=sqlite,
        project_dir=dataset_dir,
    )

    console.print(f"[green]Initialized fdl project: {name}[/green]")


@app.command()
def pull(
    source: str = typer.Argument(..., help="Target name (e.g. default)"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force re-download even if up to date"
    ),
) -> None:
    """Pull DuckLake catalog from a target."""
    import fdl

    try:
        fdl.pull(source, force=force)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None


@app.command()
def push(
    dest: str = typer.Argument(..., help="Target name (e.g. default)"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Override conflict detection"
    ),
) -> None:
    """Push catalog to a target."""
    import fdl
    from fdl.meta import PushConflictError

    try:
        fdl.push(dest, force=force)
    except PushConflictError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    except ValueError as e:
        raise typer.BadParameter(str(e)) from None


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
    import fdl

    target, cmd = _parse_command_args(ctx.args)
    try:
        raise SystemExit(fdl.run(target, cmd))
    except ValueError as e:
        raise typer.BadParameter(_command_missing_hint(str(e))) from None


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def sync(
    ctx: typer.Context,
    force: bool = typer.Option(
        False, "--force", "-f", help="Override conflict detection on push"
    ),
) -> None:
    """Run pipeline and push in one step.

    \b
    Uses command from fdl.toml, or specify explicitly:
      fdl sync TARGET
      fdl sync TARGET -- COMMAND [ARGS...]
    """
    import fdl
    from fdl.meta import PushConflictError

    target, cmd = _parse_command_args(ctx.args)
    try:
        raise SystemExit(fdl.sync(target, cmd, force=force))
    except PushConflictError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    except ValueError as e:
        raise typer.BadParameter(_command_missing_hint(str(e))) from None


def _command_missing_hint(msg: str) -> str:
    """Add CLI-specific usage hint to a 'no command' error."""
    return (
        f"{msg}\n"
        "  Either use: fdl <command> TARGET -- COMMAND\n"
        "  Or add to fdl.toml:\n"
        '    command = "python main.py"'
    )


def _parse_command_args(args: list[str]) -> tuple[str, list[str] | None]:
    """Parse TARGET [-- COMMAND] from raw args.

    When no ``--`` separator is given, returns (target, None) so the API
    falls back to the ``command`` field in fdl.toml.
    """
    if "--" in args:
        idx = args.index("--")
        before, cmd = args[:idx], args[idx + 1 :]
        if len(before) != 1:
            raise typer.BadParameter("Usage: fdl <command> TARGET [-- COMMAND]")
        if not cmd:
            raise typer.BadParameter("No command after --")
        return before[0], cmd

    if len(args) != 1:
        raise typer.BadParameter("Usage: fdl <command> TARGET [-- COMMAND]")
    return args[0], None


@app.command("config")
def config_cmd(
    key: str = typer.Argument(None, help="Config key (e.g. 'targets.default.url')"),
    value: str = typer.Argument(None, help="Value to set"),
) -> None:
    """Get or set fdl configuration (reads/writes fdl.toml)."""
    from fdl.config import (
        _load_toml,
        get_all,
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
    import fdl
    from fdl.config import datasource_name
    from fdl.meta import catalog_is_stale

    resolved = _resolve_target(target)
    datasource = datasource_name()

    if not force and catalog_is_stale(target, resolved, datasource):
        console.print(
            "[red]Remote catalog has been updated since the last pull. "
            "Run 'fdl pull' first, or use --force to override.[/red]"
        )
        raise SystemExit(1)

    with fdl.connect(target) as conn:
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
def duckdb(
    target: str = typer.Argument(..., help="Target name (e.g. default)"),
    read_only: bool = typer.Option(
        False, "--read-only", help="Open the catalog in read-only mode"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Skip stale catalog check"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Print the duckdb command that would be executed and exit",
    ),
    duckdb_bin: str = typer.Option(
        "duckdb",
        "--duckdb-bin",
        help="Path to the duckdb binary (default: first on PATH)",
    ),
) -> None:
    """Launch an interactive DuckDB shell with the DuckLake catalog attached."""
    import os
    import shlex

    from fdl.config import datasource_name
    from fdl.ducklake import build_attach_sql
    from fdl.meta import catalog_is_stale

    resolved = _resolve_target(target)
    datasource = datasource_name()

    if not force and catalog_is_stale(target, resolved, datasource):
        console.print(
            "[red]Remote catalog has been updated since the last pull. "
            "Run 'fdl pull' first, or use --force to override.[/red]"
        )
        raise SystemExit(1)

    stmts = build_attach_sql(target, read_only=read_only)
    argv = [duckdb_bin]
    for s in stmts:
        argv += ["-cmd", s]

    if dry_run:
        print(shlex.join(argv))
        return

    try:
        os.execvp(duckdb_bin, argv)
    except FileNotFoundError:
        console.print(
            f"[red]duckdb binary '{duckdb_bin}' not found on PATH. "
            f"Install DuckDB CLI or pass --duckdb-bin.[/red]"
        )
        raise SystemExit(127)


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
