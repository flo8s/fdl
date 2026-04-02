import os
from pathlib import Path

FDL_DIR = Path(".fdl")
DUCKLAKE_FILE = "ducklake.duckdb"
DUCKLAKE_SQLITE = "ducklake.sqlite"
META_JSON = "meta.json"


def fdl_target_dir(target_name: str) -> Path:
    """Target-specific directory under .fdl/."""
    return FDL_DIR / target_name


def ducklake_data_path(catalog_url: str) -> str:
    """Derive DuckLake DATA_PATH from a catalog URL or path."""
    return f"{catalog_url}.files/"


def default_target_url() -> str:
    """Default target URL ($XDG_DATA_HOME/fdl or ~/.local/share/fdl).

    Returns a display-friendly path using ~ when under the home directory.
    """
    xdg = os.environ.get("XDG_DATA_HOME")
    result = Path(xdg, "fdl") if xdg else Path.home() / ".local" / "share" / "fdl"
    home = Path.home()
    try:
        return str(Path("~") / result.relative_to(home))
    except ValueError:
        return str(result)
