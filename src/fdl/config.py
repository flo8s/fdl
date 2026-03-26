"""fdl config management (3-layer: fdl.toml → .fdl/config → ~/.fdl/config)."""

import os
import tomllib
from pathlib import Path

PROJECT_CONFIG = "fdl.toml"


def user_config_path() -> Path:
    """User-level config (~/.fdl/config). S3 credentials, personal settings."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg) / "fdl" / "config"
    return Path.home() / ".fdl" / "config"


def workspace_config_path() -> Path:
    """Workspace-level config (.fdl/config). Local overrides, not tracked."""
    from fdl import FDL_DIR

    return Path.cwd() / FDL_DIR / "config"


def project_config_path() -> Path:
    """Project-level config (fdl.toml). Team-shared, tracked in git."""
    return Path.cwd() / PROJECT_CONFIG


def load_toml(path: Path) -> dict:
    """Load a TOML file, returning empty dict if not found."""
    if not path.exists():
        return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def get(key: str) -> str | None:
    """Get a config value by dotted key (e.g. 's3.endpoint')."""
    section, name = _parse_key(key)
    data = load_toml(user_config_path())
    return data.get(section, {}).get(name)


def set_value(key: str, value: str, path: Path | None = None) -> None:
    """Set a config value by dotted key."""
    section, name = _parse_key(key)
    target = path or user_config_path()
    data = load_toml(target)

    if section not in data:
        data[section] = {}
    data[section][name] = value

    _write_toml(target, data)


def get_all(path: Path | None = None) -> dict[str, str]:
    """Get all config values as flat dotted keys."""
    data = load_toml(path or user_config_path())
    result = {}
    for section, values in data.items():
        if isinstance(values, dict):
            for k, v in values.items():
                result[f"{section}.{k}"] = str(v)
    return result


def storage() -> str:
    """FDL_STORAGE: base path for data files (env var or default .fdl)."""
    from fdl import FDL_DIR

    return os.environ.get("FDL_STORAGE", str(FDL_DIR))


def data_path() -> str:
    """FDL_DATA_PATH: path to data files directory."""
    from fdl import DUCKLAKE_FILE, ducklake_data_path

    return os.environ.get("FDL_DATA_PATH") or ducklake_data_path(f"{storage()}/{DUCKLAKE_FILE}")


def attach_path() -> str:
    """FDL_ATTACH_PATH: DuckLake attach path for dbt (auto-detect sqlite/duckdb)."""
    if v := os.environ.get("FDL_ATTACH_PATH"):
        return v
    from fdl import FDL_DIR, DUCKLAKE_FILE, DUCKLAKE_SQLITE

    sqlite = FDL_DIR / DUCKLAKE_SQLITE
    if sqlite.exists():
        return f"ducklake:sqlite:{sqlite}"
    return f"ducklake:{FDL_DIR / DUCKLAKE_FILE}"


def fdl_env_dict(*, storage_override: str | None = None) -> dict[str, str]:
    """All FDL_* settings as env var dict (for fdl run subprocess)."""
    from fdl import DUCKLAKE_FILE, ducklake_data_path

    storage_val = storage_override or storage()
    return {
        "FDL_STORAGE": storage_val,
        "FDL_DATA_PATH": ducklake_data_path(f"{storage_val}/{DUCKLAKE_FILE}"),
        "FDL_ATTACH_PATH": attach_path(),
        **s3_env_dict(),
    }


def _resolve(env_key: str, section: str, name: str) -> str | None:
    """Resolve a value: env var → .fdl/config → ~/.fdl/config."""
    return os.environ.get(env_key) or _get_config_value(section, name)


def _get_config_value(section: str, name: str) -> str | None:
    """Look up a value from workspace config, then user config."""
    for path in [workspace_config_path(), user_config_path()]:
        data = load_toml(path)
        value = data.get(section, {}).get(name)
        if value:
            return value
    return None


def s3_endpoint() -> str:
    """FDL_S3_ENDPOINT env var, or s3.endpoint from user config."""
    v = _resolve("FDL_S3_ENDPOINT", "s3", "endpoint")
    if not v:
        raise KeyError("S3 endpoint not configured. Set FDL_S3_ENDPOINT or: fdl config s3.endpoint <value>")
    return v


def s3_access_key_id() -> str:
    """FDL_S3_ACCESS_KEY_ID env var, or s3.access_key_id from user config."""
    v = _resolve("FDL_S3_ACCESS_KEY_ID", "s3", "access_key_id")
    if not v:
        raise KeyError("S3 access key not configured. Set FDL_S3_ACCESS_KEY_ID or: fdl config s3.access_key_id <value>")
    return v


def s3_secret_access_key() -> str:
    """FDL_S3_SECRET_ACCESS_KEY env var, or s3.secret_access_key from user config."""
    v = _resolve("FDL_S3_SECRET_ACCESS_KEY", "s3", "secret_access_key")
    if not v:
        raise KeyError("S3 secret key not configured. Set FDL_S3_SECRET_ACCESS_KEY or: fdl config s3.secret_access_key <value>")
    return v


def s3_env_dict() -> dict[str, str]:
    """All S3 settings as {FDL_S3_*: value} dict (for fdl run subprocess)."""
    result = {}
    for env_key, section, name in [
        ("FDL_S3_ENDPOINT", "s3", "endpoint"),
        ("FDL_S3_ACCESS_KEY_ID", "s3", "access_key_id"),
        ("FDL_S3_SECRET_ACCESS_KEY", "s3", "secret_access_key"),
    ]:
        v = _resolve(env_key, section, name)
        if v:
            result[env_key] = v
    return result


def resolve_remote(name: str, project_dir: Path) -> str:
    """Resolve a remote name to a concrete URL or path.

    1. fdl.toml (project, tracked)
    2. .fdl/config (workspace, not tracked)
    3. ~/.fdl/config (user)
    """
    # Reject direct URLs/paths with helpful hint
    if name.startswith("s3://") or name.startswith("/") or name.startswith("."):
        raise ValueError(
            f"'{name}' looks like a URL or path. Register it as a remote:\n"
            f"  fdl config --local remotes.origin \"{name}\"\n"
            f"  fdl push origin"
        )

    for path in [
        project_dir / PROJECT_CONFIG,
        workspace_config_path(),
        user_config_path(),
    ]:
        data = load_toml(path)
        url = data.get("remotes", {}).get(name)
        if url:
            return url

    raise ValueError(
        f"remote '{name}' not found.\n"
        f"  Define it in {PROJECT_CONFIG} or {user_config_path()}"
    )


def _parse_key(key: str) -> tuple[str, str]:
    """Parse 'section.name' into (section, name)."""
    parts = key.split(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid key '{key}'. Use 'section.name' format (e.g. 's3.endpoint')")
    return parts[0], parts[1]


def _write_toml(path: Path, data: dict) -> None:
    """Write a simple TOML file (flat sections with string values)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for section, values in data.items():
        if not isinstance(values, dict):
            continue
        lines.append(f"[{section}]")
        for k, v in values.items():
            lines.append(f'{k} = "{_escape_toml_string(v)}"')
        lines.append("")
    path.write_text("\n".join(lines))


def _escape_toml_string(s: str) -> str:
    """Escape special characters for TOML basic strings."""
    return s.replace("\\", "\\\\").replace('"', '\\"')
