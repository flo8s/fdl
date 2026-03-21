"""Pydantic model definitions for dataset configuration (dataset.yml)."""

import os
from pathlib import Path

import yaml
from jinja2 import Environment
from pydantic import BaseModel

from fdl import DATASET_YML, DUCKLAKE_FILE


def _env_var(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value is not None:
        return value
    if default is not None:
        return default
    raise ValueError(f"env var '{name}' is not set and no default provided")


def _render_template(text: str) -> str:
    env = Environment()
    env.globals["env_var"] = _env_var
    return env.from_string(text).render()


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
    public_url: str
    s3_url: str = ""
    repository_url: str = ""
    schemas: dict[str, DatasetSchemaConfig] = {}
    dependencies: list[DependencyInfo] | None = None

    @property
    def ducklake_url(self) -> str:
        return f"{self.public_url}/{self.name}/{DUCKLAKE_FILE}"


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
