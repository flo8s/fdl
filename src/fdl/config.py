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
        # sqlite:///relative  → path = ./relative
        # sqlite:////abs/path → path = /abs/path
        path = parsed.netloc + parsed.path
        path = path.lstrip("/") if not path.startswith("//") else path
        # Restore leading / for absolute paths: sqlite:////abs → path = /abs
        if url.startswith("sqlite:////"):
            path = "/" + path.lstrip("/")
        return CatalogSpec(scheme="sqlite", raw=url, path=path)

    if scheme == "duckdb":
        path = parsed.netloc + parsed.path
        path = path.lstrip("/") if not url.startswith("duckdb:////") else "/" + path.lstrip("/")
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


def metadata_url(project_dir: Path | None = None) -> str:
    """[metadata].url from fdl.toml (with ${VAR} expansion)."""
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


def data_url_v11(project_dir: Path | None = None) -> str:
    """[data].url from fdl.toml (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("data")
    if not isinstance(section, dict) or not section.get("url"):
        raise KeyError(
            f"[data].url not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return os.path.expandvars(section["url"])


def data_s3_config(project_dir: Path | None = None) -> "S3Config | None":
    """S3 credentials from [data] section, None if [data].url is not s3://."""
    url = data_url_v11(project_dir)
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
    """[publishes.<name>].url from fdl.toml (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    section = data.get("publishes", {}).get(name)
    if not isinstance(section, dict) or not section.get("url"):
        raise KeyError(
            f"[publishes.{name}].url not found in {PROJECT_CONFIG}."
        )
    return os.path.expandvars(section["url"])


def publish_public_url(name: str, project_dir: Path | None = None) -> str | None:
    """[publishes.<name>].public_url from fdl.toml, or None."""
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
    """S3 credentials for a publish destination.

    Falls back to [data] credentials if [publishes.<name>] does not override them.
    Returns None when publishes.<name>.url is not s3://.
    """
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
    """Project config (fdl.toml).

    Searches from cwd up the directory tree. If not found anywhere,
    falls back to ``Path.cwd() / fdl.toml`` (used by ``fdl init``).
    """
    if project_dir is not None:
        return project_dir / PROJECT_CONFIG
    try:
        return find_project_dir() / PROJECT_CONFIG
    except FileNotFoundError:
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
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    name = data.get("name")
    if not name:
        raise FileNotFoundError(
            f"'name' not found in {PROJECT_CONFIG}. Run 'fdl init' first."
        )
    return name


def storage(target_name: str | None = None) -> str:
    """Default base path for data files (``.fdl/{target}``). Internal helper."""
    from fdl import FDL_DIR, fdl_target_dir

    base = fdl_target_dir(target_name) if target_name else FDL_DIR
    return str(base)


def catalog_path(
    target_name: str | None = None,
    project_dir: Path | None = None,
) -> str:
    """FDL_CATALOG_PATH: absolute path to the local SQLite DuckLake catalog.

    The local catalog is always ``ducklake.sqlite``. A legacy ``ducklake.duckdb``
    in the target directory is ignored; migrate it by running
    ``fdl pull <target> --force``.
    """
    if v := os.environ.get("FDL_CATALOG_PATH"):
        return v
    from fdl import DUCKLAKE_SQLITE, FDL_DIR, fdl_target_dir

    rel = fdl_target_dir(target_name) if target_name else FDL_DIR
    base = (project_dir / rel) if project_dir else rel
    return str(base / DUCKLAKE_SQLITE)


def catalog_url(
    target_name: str | None = None,
    project_dir: Path | None = None,
) -> str:
    """FDL_CATALOG_URL: ``sqlite:///<absolute_posix_path>`` for the local catalog."""
    if v := os.environ.get("FDL_CATALOG_URL"):
        return v
    path = Path(catalog_path(target_name, project_dir)).resolve()
    return f"sqlite:///{path.as_posix()}"


def data_url(
    target_name: str | None = None,
    *,
    storage_override: str | None = None,
) -> str:
    """FDL_DATA_URL: Parquet data files directory.

    Local target: ``.fdl/{target}/ducklake.duckdb.files/``
    S3    target: ``s3://bucket/{datasource}/ducklake.duckdb.files/``
    """
    if v := os.environ.get("FDL_DATA_URL"):
        return v
    from fdl import DUCKLAKE_FILE, ducklake_data_path

    storage_val = storage_override or storage(target_name)
    return ducklake_data_path(f"{storage_val}/{DUCKLAKE_FILE}")


def data_bucket_and_prefix(
    target_name: str,
    project_dir: Path | None = None,
) -> tuple[str, str] | None:
    """Parse (bucket, prefix) from the effective data URL for S3 targets.

    Returns ``None`` for non-S3 targets. The prefix always ends with
    ``ducklake.duckdb.files/``.
    """
    project_dir = project_dir or find_project_dir()
    resolved = resolve_target(target_name, project_dir)
    if not resolved.startswith("s3://"):
        return None
    from fdl import DUCKLAKE_FILE

    datasource = datasource_name(project_dir)
    rest = resolved.removeprefix("s3://")
    bucket, _, top_prefix = rest.partition("/")
    base_prefix = f"{top_prefix}/{datasource}" if top_prefix else datasource
    return bucket, f"{base_prefix}/{DUCKLAKE_FILE}.files/"


def target_s3_config(name: str, project_dir: Path | None = None) -> "S3Config":
    """Get S3 config for a target from fdl.toml (with ${VAR} expansion)."""
    project_dir = project_dir or find_project_dir()

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
    *,
    target_name: str | None = None,
    storage_override: str | None = None,
    project_dir: Path | None = None,
) -> dict[str, str]:
    """All FDL_* settings as env var dict (for fdl run subprocess).

    Always-present keys:
      - FDL_CATALOG_URL, FDL_CATALOG_PATH, FDL_DATA_URL

    S3-only keys:
      - FDL_DATA_BUCKET, FDL_DATA_PREFIX
      - FDL_S3_ENDPOINT, FDL_S3_ENDPOINT_HOST,
        FDL_S3_ACCESS_KEY_ID, FDL_S3_SECRET_ACCESS_KEY
    """
    result = {
        "FDL_CATALOG_URL": catalog_url(target_name, project_dir),
        "FDL_CATALOG_PATH": str(
            Path(catalog_path(target_name, project_dir)).resolve()
        ),
        "FDL_DATA_URL": data_url(
            target_name, storage_override=storage_override
        ),
    }
    if target_name:
        try:
            parts = data_bucket_and_prefix(target_name, project_dir)
            if parts is not None:
                bucket, prefix = parts
                result["FDL_DATA_BUCKET"] = bucket
                result["FDL_DATA_PREFIX"] = prefix
            s3 = target_s3_config(target_name, project_dir)
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


def target_storage_url(name: str, project_dir: Path | None = None) -> str:
    """Effective storage URL for a target: resolved target URL + datasource."""
    project_dir = project_dir or find_project_dir()
    resolved = resolve_target(name, project_dir)
    datasource = datasource_name(project_dir)
    return f"{resolved}/{datasource}"


def resolve_target(name: str, project_dir: Path | None = None) -> str:
    """Resolve a target name to a concrete URL or path from fdl.toml."""
    # Reject direct URLs/paths with helpful hint
    if name.startswith("s3://") or name.startswith("/") or name.startswith("."):
        raise ValueError(
            f"'{name}' looks like a URL or path. Register it as a target:\n"
            f'  fdl config targets.{name.split("/")[0]}.url "{name}"'
        )

    project_dir = project_dir or find_project_dir()
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
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    target = data.get("targets", {}).get(name)
    if isinstance(target, dict):
        pub = target.get("public_url")
        if pub:
            return os.path.expandvars(pub)
    return None


def target_command(target_name: str, project_dir: Path | None = None) -> str | None:
    """Pipeline command from fdl.toml. Checks targets.<name>.command, then top-level command."""
    project_dir = project_dir or find_project_dir()
    data = _load_toml(project_dir / PROJECT_CONFIG)
    # 1. targets.<name>.command
    target = data.get("targets", {}).get(target_name)
    if isinstance(target, dict):
        cmd = target.get("command")
        if cmd:
            return cmd
    # 2. Top-level command
    return data.get("command")


def ducklake_url(
    datasource: str, target_name: str, project_dir: Path | None = None
) -> str:
    """Public DuckLake catalog URL for a datasource, resolved from a target's public_url."""
    from fdl import DUCKLAKE_FILE

    pub = target_public_url(target_name, project_dir)
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
