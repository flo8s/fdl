"""Pydantic model definitions for dataset configuration (dataset.yml)."""

import os
from pathlib import Path

import yaml
from jinja2 import Environment
from pydantic import BaseModel

from fdl import DATASET_YML


def _env_var(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value is not None:
        return value
    if default is not None:
        return default
    raise ValueError(f"env var '{name}' is not set and no default provided")


def _render_template(text: str) -> str:
    env = Environment()
    return env.from_string(text).render(env_var=_env_var)


class DependencyInfo(BaseModel):
    alias: str
    ducklake_url: str


class DatasetSchemaConfig(BaseModel):
    title: str = ""


class DatasetConfig(BaseModel):
    name: str = ""
    title: str = ""
    description: str = ""
    tags: list[str] = []
    cover: str = ""
    repository_url: str = ""
    schemas: dict[str, DatasetSchemaConfig] = {}
    dependencies: list[DependencyInfo] | None = None


def load_dataset_config(dataset_dir: Path) -> DatasetConfig:
    """Load and validate dataset.yml as a DatasetConfig."""
    path = dataset_dir / DATASET_YML
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")
    raw = path.read_text()
    rendered = _render_template(raw)
    config = DatasetConfig.model_validate(yaml.safe_load(rendered))
    if not config.name:
        config.name = dataset_dir.resolve().name
    return config
