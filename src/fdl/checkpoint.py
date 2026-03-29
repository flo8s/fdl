"""DuckLake CHECKPOINT maintenance."""

from __future__ import annotations

from pathlib import Path

from fdl import DUCKLAKE_FILE, FDL_DIR
from fdl.config import datasource_name, resolve_target
from fdl.console import console
from fdl.meta import check_conflict, read_remote_pushed_at


def run_checkpoint(target_name: str, *, force: bool = False) -> None:
    """Run DuckLake CHECKPOINT on a target."""
    from fdl.ducklake import connect

    dataset_dir = Path.cwd()
    datasource = datasource_name(dataset_dir)
    resolved = resolve_target(target_name, dataset_dir)

    console.print(f"[bold]--- checkpoint: {datasource} ---[/bold]")

    ducklake_file = dataset_dir / FDL_DIR / DUCKLAKE_FILE
    if not ducklake_file.exists():
        raise FileNotFoundError(f"{ducklake_file} not found. Run 'fdl pull' first.")

    # Stale catalog check
    check_conflict(
        read_remote_pushed_at(resolved, target_name, datasource), force=force,
    )

    storage = f"{resolved}/{datasource}"
    with connect(storage=storage, target_name=target_name) as conn:
        conn.execute(f"CHECKPOINT {datasource}")

    console.print("[green]Checkpoint complete.[/green]")
