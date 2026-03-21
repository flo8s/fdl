"""Pydantic model definitions for dataset metadata output (metadata.json)."""

from typing import Literal

from pydantic import BaseModel

from fdl.config_schema import DependencyInfo

Materialization = Literal["table", "view", "incremental"]


class ColumnInfo(BaseModel):
    name: str
    title: str = ""
    description: str
    data_type: str
    nullable: bool = True


class ModelInfo(BaseModel):
    name: str
    title: str
    description: str
    tags: list[str]
    license: str
    license_url: str
    source_url: str
    published: bool = False
    materialized: Materialization
    columns: list[ColumnInfo]
    sql: str | None = None


class SchemaInfo(BaseModel):
    title: str
    tables: list[ModelInfo]


class NodeConfig(BaseModel):
    materialized: str


class NodeInfo(BaseModel):
    fqn: list[str]
    resource_type: str
    config: NodeConfig
    meta: dict


class LineageInfo(BaseModel):
    parent_map: dict[str, list[str]]
    nodes: dict[str, NodeInfo]


class DatasetMetadata(BaseModel):
    title: str
    description: str
    cover: str = ""
    tags: list[str]
    ducklake_url: str
    schemas: dict[str, SchemaInfo]
    lineage: LineageInfo
    dependencies: list[DependencyInfo] | None = None
    readme: str | None = None
