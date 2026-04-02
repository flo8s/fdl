"""fdl config management (fdl.toml + ${VAR} expansion)."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fdl.s3 import S3Config

PROJECT_CONFIG = "fdl.toml"


def project_config_path() -> Path:
    """Project config (fdl.toml)."""
    return Path.cwd() / PROJECT_CONFIG


def _load_toml(path: Path) -> dict:
    """Load a TOML file."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def set_value(key: str, value: str, path: Path | None = None) -> None:
    """Set a config value. Supports dotted keys up to 3 levels (e.g. targets.default.url)."""
    dest = path or project_config_path()
    try:
        data = _load_toml(dest)
    except FileNotFoundError:
        data = {}

    parts = key.split(".")
    if len(parts) == 1:
        data[key] = value
    elif len(parts) == 2:
        section, name = parts
        if section not in data:
            data[section] = {}
        data[section][name] = value
    elif len(parts) == 3:
        section, subsection, name = parts
        if section not in data:
            data[section] = {}
        if subsection not in data[section] or not isinstance(
            data[section][subsection], dict
        ):
            data[section][subsection] = {}
        data[section][subsection][name] = value
    else:
        raise ValueError(
            f"Key too deep: '{key}'. Maximum 3 levels (e.g. targets.default.url)"
        )

    _write_toml(dest, data)


def get_all(path: Path | None = None) -> dict[str, str]:
    """Get all config values as flat dotted keys."""
    data = _load_toml(path or project_config_path())
    return _flatten(data)


def _flatten(data: dict, prefix: str = "") -> dict[str, str]:
    """Flatten a nested dict into dotted keys."""
    result = {}
    for key, value in data.items():
        full_key = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict):
            result.update(_flatten(value, f"{full_key}."))
        else:
            result[full_key] = str(value)
    return result


def datasource_name(project_dir: Path | None = None) -> str:
    """Datasource name from fdl.toml."""
    project_dir = project_dir or Path.cwd()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    name = data.get("name")
    if not name:
        raise FileNotFoundError(
            f"'name' not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return name


def catalog_type(project_dir: Path | None = None) -> str:
    """Catalog type from fdl.toml ('duckdb' or 'sqlite')."""
    project_dir = project_dir or Path.cwd()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    return data.get("catalog", "duckdb")


def storage(target_name: str | None = None) -> str:
    """FDL_STORAGE: base path for data files (env var or default .fdl/{target})."""
    from fdl import FDL_DIR, fdl_target_dir

    base = fdl_target_dir(target_name) if target_name else FDL_DIR
    return os.environ.get("FDL_STORAGE", str(base))


def data_path(target_name: str | None = None) -> str:
    """FDL_DATA_PATH: path to data files directory."""
    from fdl import DUCKLAKE_FILE, ducklake_data_path

    return os.environ.get("FDL_DATA_PATH") or ducklake_data_path(
        f"{storage(target_name)}/{DUCKLAKE_FILE}"
    )


def catalog_path(target_name: str | None = None) -> str:
    """FDL_CATALOG: path to the DuckLake catalog file (auto-detect sqlite/duckdb)."""
    if v := os.environ.get("FDL_CATALOG"):
        return v
    from fdl import DUCKLAKE_FILE, DUCKLAKE_SQLITE, FDL_DIR, fdl_target_dir

    base = fdl_target_dir(target_name) if target_name else FDL_DIR
    sqlite = base / DUCKLAKE_SQLITE
    if sqlite.exists():
        return str(sqlite)
    duckdb = base / DUCKLAKE_FILE
    if duckdb.exists():
        return str(duckdb)
    # Neither exists: fall back to catalog type from fdl.toml
    if catalog_type() == "sqlite":
        return str(sqlite)
    return str(duckdb)


def target_s3_config(name: str, project_dir: Path | None = None) -> "S3Config":
    """Get S3 config for a target from fdl.toml (with ${VAR} expansion)."""
    project_dir = project_dir or Path.cwd()

    # Resolve bucket from target URL
    url = resolve_target(name, project_dir)
    if not url.startswith("s3://"):
        raise ValueError(f"Target '{name}' is not an S3 target: {url}")
    bucket = url.removeprefix("s3://")

    data = _load_toml(project_dir / PROJECT_CONFIG)
    target = data.get("targets", {}).get(name, {})

    from fdl.s3 import S3Config

    return S3Config(
        bucket=bucket,
        endpoint=os.path.expandvars(target.get("s3_endpoint", "")),
        access_key_id=os.path.expandvars(target.get("s3_access_key_id", "")),
        secret_access_key=os.path.expandvars(target.get("s3_secret_access_key", "")),
    )


def fdl_env_dict(
    *, target_name: str | None = None, storage_override: str | None = None
) -> dict[str, str]:
    """All FDL_* settings as env var dict (for fdl run subprocess)."""
    from fdl import DUCKLAKE_FILE, ducklake_data_path

    storage_val = storage_override or storage()
    result = {
        "FDL_STORAGE": storage_val,
        "FDL_DATA_PATH": ducklake_data_path(f"{storage_val}/{DUCKLAKE_FILE}"),
        "FDL_CATALOG": catalog_path(target_name),
    }
    if target_name:
        try:
            s3 = target_s3_config(target_name)
            if s3.endpoint:
                result["FDL_S3_ENDPOINT"] = s3.endpoint
                result["FDL_S3_ENDPOINT_HOST"] = s3.endpoint_host
            if s3.access_key_id:
                result["FDL_S3_ACCESS_KEY_ID"] = s3.access_key_id
            if s3.secret_access_key:
                result["FDL_S3_SECRET_ACCESS_KEY"] = s3.secret_access_key
        except ValueError:
            pass  # Not an S3 target
    return result


def resolve_target(name: str, project_dir: Path) -> str:
    """Resolve a target name to a concrete URL or path from fdl.toml."""
    # Reject direct URLs/paths with helpful hint
    if name.startswith("s3://") or name.startswith("/") or name.startswith("."):
        raise ValueError(
            f"'{name}' looks like a URL or path. Register it as a target:\n"
            f'  fdl config targets.{name.split("/")[0]}.url "{name}"'
        )

    data = _load_toml(project_dir / PROJECT_CONFIG)
    target = data.get("targets", {}).get(name)
    if isinstance(target, dict):
        url = target.get("url")
        if url:
            expanded = os.path.expandvars(url)
            if "://" in expanded:
                return expanded
            return str(Path(expanded).expanduser())

    raise ValueError(f"target '{name}' not found.\n  Define it in {PROJECT_CONFIG}")


def target_public_url(name: str, project_dir: Path | None = None) -> str | None:
    """Get the public_url for a target from fdl.toml."""
    project_dir = project_dir or Path.cwd()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    target = data.get("targets", {}).get(name)
    if isinstance(target, dict):
        pub = target.get("public_url")
        if pub:
            return os.path.expandvars(pub)
    return None


def ducklake_url(
    datasource: str, target_name: str, project_dir: Path | None = None
) -> str:
    """Public DuckLake catalog URL for a datasource, resolved from a target's public_url."""
    from fdl import DUCKLAKE_FILE

    pub = target_public_url(target_name, project_dir or Path.cwd())
    if not pub:
        raise KeyError(
            f"public_url not configured for target '{target_name}'.\n"
            f"  fdl config targets.{target_name}.public_url <value>"
        )
    return f"{pub}/{datasource}/{DUCKLAKE_FILE}"


def _write_toml(path: Path, data: dict) -> None:
    """Write a TOML file (top-level values, flat sections, and nested tables)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []

    # Top-level values first
    for k, v in data.items():
        if not isinstance(v, dict):
            lines.append(f'{k} = "{_escape_toml_string(str(v))}"')
    if lines:
        lines.append("")

    # Sections (flat and nested)
    for section, values in data.items():
        if not isinstance(values, dict):
            continue
        has_nested = any(isinstance(v, dict) for v in values.values())
        if has_nested:
            flat_values = {k: v for k, v in values.items() if not isinstance(v, dict)}
            if flat_values:
                lines.append(f"[{section}]")
                for k, v in flat_values.items():
                    lines.append(f'{k} = "{_escape_toml_string(str(v))}"')
                lines.append("")
            for subsection, subvalues in values.items():
                if isinstance(subvalues, dict):
                    lines.append(f"[{section}.{subsection}]")
                    for k, v in subvalues.items():
                        lines.append(f'{k} = "{_escape_toml_string(str(v))}"')
                    lines.append("")
        else:
            lines.append(f"[{section}]")
            for k, v in values.items():
                lines.append(f'{k} = "{_escape_toml_string(str(v))}"')
            lines.append("")

    path.write_text("\n".join(lines))


def _escape_toml_string(s: str) -> str:
    """Escape special characters for TOML basic strings."""
    return s.replace("\\", "\\\\").replace('"', '\\"')
