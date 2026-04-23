"""fdl config management (fdl.toml + ${VAR} expansion)."""

from __future__ import annotations

import os
import tomllib
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from fdl.s3 import S3Config

PROJECT_CONFIG = "fdl.toml"


# ---------------------------------------------------------------------------
# Catalog URL parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PgConnInfo:
    """Parsed components of a postgres:// URL."""

    host: str
    port: int | None
    database: str
    user: str | None
    password: str | None
    schema: str | None


@dataclass(frozen=True)
class CatalogSpec:
    """Parsed [metadata].url — sqlite / postgres / duckdb backend."""

    scheme: Literal["sqlite", "postgres", "duckdb"]
    raw: str
    path: str | None = None
    pg: PgConnInfo | None = None


def parse_catalog_url(url: str) -> CatalogSpec:
    """Parse a catalog URL into a CatalogSpec. ${VAR} must already be expanded."""
    if "://" not in url:
        raise ValueError(
            f"Catalog URL must include a scheme (sqlite://, postgres://, duckdb://): {url!r}"
        )
    parsed = urllib.parse.urlsplit(url)
    scheme = parsed.scheme

    if scheme == "sqlite":
        path = parsed.netloc + parsed.path
        path = path.lstrip("/") if not path.startswith("//") else path
        if url.startswith("sqlite:////"):
            path = "/" + path.lstrip("/")
        return CatalogSpec(scheme="sqlite", raw=url, path=path)

    if scheme == "duckdb":
        path = parsed.netloc + parsed.path
        path = (
            "/" + path.lstrip("/")
            if url.startswith("duckdb:////")
            else path.lstrip("/")
        )
        return CatalogSpec(scheme="duckdb", raw=url, path=path)

    if scheme in ("postgres", "postgresql"):
        host = parsed.hostname or "localhost"
        port = parsed.port
        database = (parsed.path or "").lstrip("/") or ""
        if not database:
            raise ValueError(f"postgres URL missing database: {url!r}")
        user = urllib.parse.unquote(parsed.username) if parsed.username else None
        password = urllib.parse.unquote(parsed.password) if parsed.password else None
        query = urllib.parse.parse_qs(parsed.query)
        schema = query.get("schema", [None])[0]
        return CatalogSpec(
            scheme="postgres",
            raw=url,
            pg=PgConnInfo(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                schema=schema,
            ),
        )

    raise ValueError(f"Unsupported catalog scheme {scheme!r}: {url!r}")


# ---------------------------------------------------------------------------
# fdl.toml locator / loader
# ---------------------------------------------------------------------------


def find_project_dir(start: Path | None = None) -> Path:
    """Find the nearest ancestor directory that contains fdl.toml."""
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / PROJECT_CONFIG).is_file():
            return candidate
    raise FileNotFoundError(
        f"{PROJECT_CONFIG} not found in {current} or any parent directory"
    )


def project_config_path(project_dir: Path | None = None) -> Path:
    """Project config path. Falls back to cwd/fdl.toml when not found."""
    if project_dir is not None:
        return project_dir / PROJECT_CONFIG
    try:
        return find_project_dir() / PROJECT_CONFIG
    except FileNotFoundError:
        return Path.cwd() / PROJECT_CONFIG


def _load_toml(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def set_value(key: str, value: str, path: Path | None = None) -> None:
    """Set a config value. Supports dotted keys up to 3 levels."""
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
            f"Key too deep: '{key}'. Maximum 3 levels (e.g. publishes.default.url)"
        )

    _write_toml(dest, data)


def get_all(path: Path | None = None) -> dict[str, str]:
    """Get all config values as flat dotted keys."""
    data = _load_toml(path or project_config_path())
    return _flatten(data)


def _flatten(data: dict, prefix: str = "") -> dict[str, str]:
    result = {}
    for key, value in data.items():
        full_key = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict):
            result.update(_flatten(value, f"{full_key}."))
        else:
            result[full_key] = str(value)
    return result


def datasource_name(project_dir: Path | None = None) -> str:
    """Datasource name from fdl.toml ``name``."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    name = data.get("name")
    if not name:
        raise FileNotFoundError(
            f"'name' not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return name


# ---------------------------------------------------------------------------
# [metadata]
# ---------------------------------------------------------------------------


def metadata_url(project_dir: Path | None = None) -> str:
    """[metadata].url (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("metadata")
    if not isinstance(section, dict) or not section.get("url"):
        raise KeyError(
            f"[metadata].url not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return os.path.expandvars(section["url"])


def metadata_spec(project_dir: Path | None = None) -> CatalogSpec:
    """Parsed [metadata] section as a CatalogSpec."""
    return parse_catalog_url(metadata_url(project_dir))


def metadata_schema(project_dir: Path | None = None) -> str | None:
    """[metadata].schema override, or None if not set."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("metadata")
    if isinstance(section, dict):
        s = section.get("schema")
        if s:
            return os.path.expandvars(s)
    return None


# ---------------------------------------------------------------------------
# [data]
# ---------------------------------------------------------------------------


def data_url(project_dir: Path | None = None) -> str:
    """[data].url (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("data")
    if not isinstance(section, dict) or not section.get("url"):
        raise KeyError(
            f"[data].url not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return os.path.expandvars(section["url"])


def data_s3_config(project_dir: Path | None = None) -> "S3Config | None":
    """S3 credentials from [data], None if [data].url is not s3://."""
    url = data_url(project_dir)
    if not url.startswith("s3://"):
        return None
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("data", {})
    bucket = url.removeprefix("s3://").split("/", 1)[0]

    from fdl.s3 import S3Config

    return S3Config(
        bucket=bucket,
        endpoint=os.path.expandvars(section.get("s3_endpoint", "")),
        access_key_id=os.path.expandvars(section.get("s3_access_key_id", "")),
        secret_access_key=os.path.expandvars(section.get("s3_secret_access_key", "")),
    )


# ---------------------------------------------------------------------------
# [publishes.*]
# ---------------------------------------------------------------------------


def publish_names(project_dir: Path | None = None) -> list[str]:
    """All publish names defined in [publishes.*] (insertion order)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    pubs = data.get("publishes")
    if not isinstance(pubs, dict):
        return []
    return [name for name, val in pubs.items() if isinstance(val, dict)]


def resolve_publish_name(
    name: str | None, project_dir: Path | None = None
) -> str:
    """Resolve an optional publish name: None → implicit if exactly one exists."""
    names = publish_names(project_dir)
    if name is not None:
        if name not in names:
            raise KeyError(
                f"publish '{name}' not found in {PROJECT_CONFIG}. "
                f"Defined: {names or 'none'}"
            )
        return name
    if len(names) == 0:
        raise KeyError(
            f"No [publishes.*] defined in {PROJECT_CONFIG}. "
            f"Add one with 'fdl config publishes.<name>.url <url>'."
        )
    if len(names) == 1:
        return names[0]
    raise ValueError(
        f"Multiple publishes defined ({names}); specify one explicitly."
    )


def publish_url(name: str, project_dir: Path | None = None) -> str:
    """[publishes.<name>].url (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("publishes", {}).get(name)
    if not isinstance(section, dict) or not section.get("url"):
        raise KeyError(
            f"[publishes.{name}].url not found in {PROJECT_CONFIG}."
        )
    return os.path.expandvars(section["url"])


def publish_public_url(name: str, project_dir: Path | None = None) -> str | None:
    """[publishes.<name>].public_url, or None."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("publishes", {}).get(name)
    if isinstance(section, dict):
        pub = section.get("public_url")
        if pub:
            return os.path.expandvars(pub)
    return None


def publish_s3_config(
    name: str, project_dir: Path | None = None
) -> "S3Config | None":
    """S3 credentials for a publish destination, falling back to [data]."""
    url = publish_url(name, project_dir)
    if not url.startswith("s3://"):
        return None
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("publishes", {}).get(name, {})
    fallback = data.get("data", {})
    bucket = url.removeprefix("s3://").split("/", 1)[0]

    def _pick(key: str) -> str:
        val = section.get(key) or fallback.get(key) or ""
        return os.path.expandvars(val)

    from fdl.s3 import S3Config

    return S3Config(
        bucket=bucket,
        endpoint=_pick("s3_endpoint"),
        access_key_id=_pick("s3_access_key_id"),
        secret_access_key=_pick("s3_secret_access_key"),
    )


# ---------------------------------------------------------------------------
# Environment variables for subprocesses
# ---------------------------------------------------------------------------


def fdl_env_dict(
    *,
    publish_name: str | None = None,  # noqa: ARG001 — reserved for future use
    project_dir: Path | None = None,
) -> dict[str, str]:
    """FDL_* env vars for subprocesses, derived from [metadata]/[data]."""
    project_dir = project_dir or find_project_dir()
    m_url = metadata_url(project_dir)
    d_url = data_url(project_dir)
    result: dict[str, str] = {
        "FDL_CATALOG_URL": m_url,
        "FDL_DATA_URL": d_url,
    }
    spec = parse_catalog_url(m_url)
    if spec.scheme == "sqlite" and spec.path:
        result["FDL_CATALOG_PATH"] = str(Path(spec.path).resolve())
    if d_url.startswith("s3://"):
        bucket, _, prefix = d_url.removeprefix("s3://").partition("/")
        result["FDL_DATA_BUCKET"] = bucket
        result["FDL_DATA_PREFIX"] = prefix
        s3 = data_s3_config(project_dir)
        if s3 is not None:
            if s3.endpoint:
                result["FDL_S3_ENDPOINT"] = s3.endpoint
                result["FDL_S3_ENDPOINT_HOST"] = s3.endpoint_host
            if s3.access_key_id:
                result["FDL_S3_ACCESS_KEY_ID"] = s3.access_key_id
            if s3.secret_access_key:
                result["FDL_S3_SECRET_ACCESS_KEY"] = s3.secret_access_key
    return result


# ---------------------------------------------------------------------------
# TOML writing
# ---------------------------------------------------------------------------


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
