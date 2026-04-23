"""Pull: re-fetch the local SQLite live catalog from a published frozen snapshot.

Only meaningful when ``[metadata]`` is SQLite — a local live catalog can drift
from the publisher's frozen copy (e.g. another host published since). For
PostgreSQL metadata the live catalog is the source of truth, so pull has
nothing to fetch.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from fdl import DUCKLAKE_FILE
from fdl.console import console


def pull(
    name: str | None = None,
    *,
    project_dir: Path | None = None,
) -> None:
    """Replace the local SQLite live catalog with the published snapshot.

    Args:
        name: Publish name to pull from (default: sole [publishes.*] entry).
        project_dir: Project directory containing fdl.toml. Defaults to the
            nearest ancestor that contains one.
    """
    from fdl.clone import _fetch, _normalize_base
    from fdl.config import (
        find_project_dir,
        metadata_spec,
        publish_url,
        resolve_publish_name,
    )
    from fdl.ducklake import _convert_ducklake_catalog

    root = project_dir or find_project_dir()
    spec = metadata_spec(root)
    if spec.scheme != "sqlite":
        raise ValueError(
            "fdl pull is only supported for sqlite metadata; "
            f"current metadata is {spec.scheme}. "
            "PostgreSQL live catalogs are authoritative — no pull is needed."
        )
    assert spec.path is not None

    name = resolve_publish_name(name, root)
    base = _normalize_base(publish_url(name, root))

    console.print(f"[bold]--- pull: {name} ← {base} ---[/bold]")

    local_sqlite = Path(spec.path)
    local_sqlite.parent.mkdir(parents=True, exist_ok=True)

    # Fetch the published frozen DuckDB, then convert into local SQLite.
    # fdl.toml is not modified by pull — only the live catalog is.
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".duckdb")
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        console.print(f"  [dim]{DUCKLAKE_FILE}[/dim]")
        _fetch(base + DUCKLAKE_FILE, tmp_path)

        # Preserve DATA_PATH from the fetched catalog — the convert helper
        # writes the provided data_path into the destination, so we read it
        # from the source first to avoid clobbering.
        data_path = _read_data_path_from_duckdb(tmp_path)

        if local_sqlite.exists():
            local_sqlite.unlink()
        _convert_ducklake_catalog(
            tmp_path,
            local_sqlite,
            src_type="duckdb",
            dst_type="sqlite",
            data_path=data_path,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    console.print(f"[green]Pulled {name} from {base}[/green]")


def _read_data_path_from_duckdb(duckdb_file: Path) -> str:
    """Read ``ducklake_metadata.data_path`` from a DuckDB catalog file."""
    import duckdb

    conn = duckdb.connect(str(duckdb_file), read_only=True)
    try:
        row = conn.execute(
            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'"
        ).fetchone()
    finally:
        conn.close()
    if row is None or row[0] is None:
        raise ValueError(
            f"{duckdb_file}: ducklake_metadata.data_path is missing"
        )
    return row[0]
