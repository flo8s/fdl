"""Metadata generation: manifest + catalog → metadata.json."""

import json
import shutil
from collections import defaultdict
from pathlib import Path

from dbt.artifacts.resources.v1.model import Model
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.manifest import WritableManifest

from fdl import METADATA_JSON
from fdl.console import console
from fdl.config import ducklake_url as fdl_ducklake_url
from fdl.config_schema import DatasetConfig, load_dataset_config
from fdl.metadata_schema import (
    ColumnInfo,
    DatasetMetadata,
    LineageInfo,
    ModelInfo,
    NodeConfig,
    NodeInfo,
    SchemaInfo,
)


def _is_datasource_model(node, datasource: str) -> bool:
    if not isinstance(node, Model):
        return False
    if not node.fqn or node.fqn[0] != datasource:
        return False
    return True


def _resolve_column_type(
    col_name: str,
    col_info_data_type: str | None,
    catalog_columns: dict,
) -> str:
    """catalog (DB introspection) > manifest (YAML-declared) > empty"""
    if col_name in catalog_columns:
        catalog_type = catalog_columns[col_name].type
        if catalog_type:
            return catalog_type
    return col_info_data_type or ""


def _build_columns(node: Model, catalog_columns: dict) -> list[ColumnInfo]:
    """manifest のカラム定義と catalog の型情報を統合して ColumnInfo リストを構築する。"""
    return [
        ColumnInfo(
            name=col_name,
            title=col_info.meta.get("title", ""),
            description=col_info.description,
            data_type=_resolve_column_type(col_name, col_info.data_type, catalog_columns),
            nullable=not any(c.type.value == "not_null" for c in col_info.constraints),
        )
        for col_name, col_info in node.columns.items()
    ]


def _build_model_info(node: Model, catalog_columns: dict) -> ModelInfo:
    """manifest ノードから公開用の ModelInfo を構築する。"""
    meta = node.meta
    return ModelInfo(
        name=node.name,
        title=meta.get("title", ""),
        description=node.description,
        tags=meta.get("tags", []),
        license=meta.get("license", ""),
        license_url=meta.get("license_url", ""),
        source_url=meta.get("source_url", ""),
        published=meta.get("published", False),
        columns=_build_columns(node, catalog_columns),
        materialized=node.config.materialized,
        sql=(node.compiled_code or "").strip() or None,
        file_path=node.original_file_path or "",
    )


def extract_models(
    manifest: WritableManifest, catalog: CatalogArtifact | None, datasource: str
) -> dict[str, list[ModelInfo]]:
    """Extract models for the given datasource from manifest.json and group by schema."""
    tables_by_schema: dict[str, list[ModelInfo]] = defaultdict(list)
    for node_id, node in manifest.nodes.items():
        if not _is_datasource_model(node, datasource):
            continue
        catalog_node = catalog.nodes.get(node_id) if catalog else None
        catalog_columns = catalog_node.columns if catalog_node else {}
        tables_by_schema[node.schema].append(
            _build_model_info(node, catalog_columns)
        )
    return dict(tables_by_schema)


def extract_lineage(manifest: WritableManifest, datasource: str) -> LineageInfo:
    """Extract lineage (DAG) information from manifest.json."""
    prefix = f"model.{datasource}."
    parent_map_raw = manifest.parent_map or {}

    parent_map: dict[str, list[str]] = {}
    node_keys: set[str] = set()

    for full_key, parents in parent_map_raw.items():
        if not full_key.startswith(prefix):
            continue
        short_key = full_key[len(prefix) :]
        short_parents = [p[len(prefix) :] for p in parents if p.startswith(prefix)]
        parent_map[short_key] = short_parents
        node_keys.add(short_key)
        node_keys.update(short_parents)

    nodes: dict[str, NodeInfo] = {}
    for key in node_keys:
        full_key = prefix + key
        node = manifest.nodes.get(full_key)
        if node:
            nodes[key] = NodeInfo(
                fqn=node.fqn,
                resource_type=node.resource_type,
                config=NodeConfig(materialized=node.config.materialized),
                meta=node.meta,
            )
        else:
            nodes[key] = NodeInfo(
                fqn=[],
                resource_type="model",
                config=NodeConfig(materialized="view"),
                meta={},
            )

    return LineageInfo(parent_map=parent_map, nodes=nodes)


def build_metadata(
    dataset_config: DatasetConfig,
    manifest: WritableManifest,
    catalog: CatalogArtifact | None,
    datasource: str,
    readme: str | None = None,
) -> DatasetMetadata:
    """Pure function: DatasetConfig → DatasetMetadata"""
    tables_by_schema = extract_models(manifest, catalog, datasource)
    lineage = extract_lineage(manifest, datasource)

    schemas: dict[str, SchemaInfo] = {}
    for name, info in dataset_config.schemas.items():
        schemas[name] = SchemaInfo(
            title=info.title,
            tables=tables_by_schema.get(name, []),
        )
    for name, tables in tables_by_schema.items():
        if name not in schemas:
            schemas[name] = SchemaInfo(title="", tables=tables)

    return DatasetMetadata(
        title=dataset_config.title,
        description=dataset_config.description,
        cover=dataset_config.cover,
        tags=dataset_config.tags,
        ducklake_url=fdl_ducklake_url(datasource),
        repository_url=dataset_config.repository_url,
        schemas=schemas,
        lineage=lineage,
        dependencies=dataset_config.dependencies,
        readme=readme,
    )


def load_manifest(target_dir: Path) -> WritableManifest:
    """Load dbt manifest.json from the target directory."""
    path = target_dir / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run dbt run first.")
    return WritableManifest.read_and_check_versions(str(path))


def load_catalog(target_dir: Path) -> CatalogArtifact | None:
    """Load dbt catalog.json if it exists."""
    path = target_dir / "catalog.json"
    if not path.exists():
        return None
    return CatalogArtifact.read_and_check_versions(str(path))


def generate_metadata(dataset_dir: Path, dist_dir: Path, target_dir: Path) -> None:
    """I/O wrapper: ファイル読み書き + ログ出力"""
    from fdl.config import datasource_name

    dataset_config = load_dataset_config(dataset_dir)
    datasource = datasource_name(dataset_dir)
    manifest = load_manifest(target_dir)
    catalog = load_catalog(target_dir)

    readme_path = dataset_dir / "README.md"
    readme = readme_path.read_text() if readme_path.exists() else None

    output = build_metadata(dataset_config, manifest, catalog, datasource, readme=readme)

    dist_dir.mkdir(parents=True, exist_ok=True)
    output_path = dist_dir / METADATA_JSON
    with open(output_path, "w") as f:
        json.dump(output.model_dump(exclude_none=True), f, ensure_ascii=False, indent=2)

    total_tables = sum(len(s.tables) for s in output.schemas.values())
    console.print(f"[green]Generated metadata: {output_path}[/green]")
    console.print(f"  Datasource: {datasource} / Public tables: [bold]{total_tables}[/bold]")


def _copy_docs_to_dist(target_dir: Path, dist_dir: Path) -> None:
    """Copy dbt docs files from target directory to dist/docs/."""
    docs_dir = dist_dir / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    for name in ("index.html", "manifest.json", "catalog.json"):
        src = target_dir / name
        if src.exists():
            shutil.copy2(src, docs_dir / name)
