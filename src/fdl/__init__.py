import os
from pathlib import Path

DATASET_YML = "dataset.yml"
METADATA_JSON = "metadata.json"
FDL_DIR = Path(".fdl")
DUCKLAKE_FILE = "ducklake.duckdb"
DUCKLAKE_SQLITE = "ducklake.sqlite"


def ducklake_data_path(catalog_url: str) -> str:
    """Derive DuckLake DATA_PATH from a catalog URL or path."""
    return f"{catalog_url}.files/"


def user_dir() -> Path:
    """User-level fdl directory ($XDG_STATE_HOME/fdl or ~/.fdl)."""
    xdg = os.environ.get("XDG_STATE_HOME")
    if xdg:
        return Path(xdg) / "fdl"
    return Path.home() / ".fdl"


