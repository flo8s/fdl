"""fdl CLI entry point."""

from __future__ import annotations

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
    name: str = typer.Argument(None, help="Datasource name"),
    metadata_url: str = typer.Option(
        None, "--metadata-url", help="Metadata catalog URL (sqlite:// or postgres://)"
    ),
    data_url: str = typer.Option(
        None, "--data-url", help="Data file storage URL (local path or s3://)"
    ),
    publish_url: str = typer.Option(
        None, "--publish-url", help="Publish destination URL (optional)"
    ),
    publish_name: str = typer.Option(
        "default", "--publish-name", help="Publish section name"
    ),
) -> None:
    """Initialize a new fdl project."""
    import fdl
    from fdl.config import PROJECT_CONFIG

    dataset_dir = Path.cwd()
    if name is None:
        default = _sanitize_name(dataset_dir.resolve().name)
        name = typer.prompt("Datasource name", default=default)
    elif _sanitize_name(name) != name:
        raise typer.BadParameter(
            f"'{name}' is not a valid SQL identifier. "
            f"Use '{_sanitize_name(name)}' instead."
        )

    if (dataset_dir / PROJECT_CONFIG).exists():
        raise typer.BadParameter(f"{PROJECT_CONFIG} already exists")

    try:
        fdl.init(
            name,
            metadata_url=metadata_url,
            data_url=data_url,
            publish_url=publish_url,
            publish_name=publish_name,
            project_dir=dataset_dir,
        )
    except (ValueError, RuntimeError) as e:
        raise typer.BadParameter(str(e)) from None


@app.command()
def pull(
    name: str = typer.Argument(
        None, help="Publish name (default: sole [publishes.*] entry)"
    ),
) -> None:
    """Rebuild the local SQLite live catalog from a publish target."""
    import fdl

    try:
        fdl.pull(name)
    except (KeyError, ValueError, FileNotFoundError) as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None


@app.command()
def publish(
    name: str = typer.Argument(
        None, help="Publish name (default: sole [publishes.*] entry)"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Override ETag precondition on S3 upload"
    ),
) -> None:
    """Convert the live catalog to a frozen DuckDB and upload it."""
    import fdl
    from fdl.meta import PushConflictError

    try:
        fdl.publish(name, force=force)
    except PushConflictError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    except (KeyError, ValueError, FileNotFoundError) as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None


@app.command(
    context_settings={"allow_extra_args": True, "allow_interspersed_args": False}
)
def run(ctx: typer.Context) -> None:
    """Run a pipeline and publish on success.

    \b
    Usage:
      fdl run                    # uses command from fdl.toml
      fdl run NAME                # publishes to [publishes.NAME]
      fdl run [NAME] -- COMMAND   # explicit command
    """
    import fdl

    publish_name, cmd = _parse_run_args(ctx.args)
    try:
        raise SystemExit(fdl.run(publish_name, cmd))
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None
    except ValueError as e:
        raise typer.BadParameter(_command_missing_hint(str(e))) from None


def _parse_run_args(args: list[str]) -> tuple[str | None, list[str] | None]:
    """Parse ``[PUBLISH_NAME] [-- COMMAND]``."""
    if "--" in args:
        idx = args.index("--")
        before, cmd = args[:idx], args[idx + 1 :]
        if len(before) > 1:
            raise typer.BadParameter("Usage: fdl run [NAME] [-- COMMAND]")
        if not cmd:
            raise typer.BadParameter("No command after --")
        return (before[0] if before else None, cmd)

    if len(args) > 1:
        raise typer.BadParameter("Usage: fdl run [NAME] [-- COMMAND]")
    return (args[0] if args else None, None)


def _command_missing_hint(msg: str) -> str:
    return (
        f"{msg}\n"
        "  Either use: fdl run [NAME] -- COMMAND\n"
        "  Or add to fdl.toml:\n"
        '    command = "python main.py"'
    )


@app.command("config")
def config_cmd(
    key: str = typer.Argument(None, help="Config key (e.g. 'metadata.url')"),
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
    query: str = typer.Argument(..., help="SQL query to execute"),
) -> None:
    """Execute a SQL query against the live DuckLake catalog."""
    import fdl

    try:
        with fdl.connect() as conn:
            conn.execute(query)
            if not conn.description:
                return
            columns = [desc[0] for desc in conn.description]
            rows = conn.fetchall()
            if not rows:
                return
            col_widths = [len(c) for c in columns]
            for row in rows:
                for i, val in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(val)))
            header = " | ".join(
                c.ljust(w) for c, w in zip(columns, col_widths, strict=False)
            )
            separator = "-+-".join("-" * w for w in col_widths)
            print(header)
            print(separator)
            for row in rows:
                print(
                    " | ".join(
                        str(v).ljust(w) for v, w in zip(row, col_widths, strict=False)
                    )
                )
    except (FileNotFoundError, KeyError) as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None


@app.command()
def duckdb(
    read_only: bool = typer.Option(
        False, "--read-only", help="Open the catalog in read-only mode"
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
    """Launch an interactive DuckDB shell with the live catalog attached."""
    import os
    import shlex

    from fdl.config import (
        data_s3_config,
        data_url,
        datasource_name,
        find_project_dir,
        metadata_schema,
        metadata_spec,
    )
    from fdl.ducklake import build_attach_sql

    try:
        root = find_project_dir()
        spec = metadata_spec(root)
        stmts = build_attach_sql(
            metadata=spec,
            data_url=data_url(root),
            datasource=datasource_name(root),
            read_only=read_only,
            metadata_schema=metadata_schema(root),
            data_s3_config=data_s3_config(root),
        )
    except (FileNotFoundError, KeyError, ValueError) as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None

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
        raise SystemExit(127) from None


@app.command()
def serve(
    name: str = typer.Argument(
        None, help="Publish name (default: sole [publishes.*] entry)"
    ),
    port: int = typer.Option(4001, help="Port number"),
) -> None:
    """Serve a local publish directory over HTTP (CORS + Range support)."""
    from fdl.config import find_project_dir, publish_url, resolve_publish_name
    from fdl.serve import run_server

    try:
        root = find_project_dir()
        name = resolve_publish_name(name, root)
        url = publish_url(name, root)
    except (KeyError, ValueError, FileNotFoundError) as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1) from None

    if url.startswith(("s3://", "http://", "https://")):
        console.print(
            f"[red]publishes.{name}.url ({url}) is remote; "
            "fdl serve only supports local paths.[/red]"
        )
        raise SystemExit(1)
    local = url.removeprefix("file://")
    run_server(Path(local), port)


main: Typer = app

if __name__ == "__main__":
    main()
