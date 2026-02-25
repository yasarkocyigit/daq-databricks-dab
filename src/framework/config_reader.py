"""
Config reader module: loads YAML configs, validates against schema,
merges environment overrides, and provides structured config objects
to Spark Declarative Pipeline notebooks.

Directory structure:
  config/tables/{source}/{database}/{schema}/{Table}.yml
  config/sources/{source}.yml
  config/gold/{model}.yml
  config/environments/{env}.yml
"""

import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml


@dataclass
class QualityExpectation:
    name: str
    constraint: str
    action: str  # warn | drop | fail


@dataclass
class StreamConfig:
    source_type: str  # eventhub | kafka | cdc | autoloader
    trigger_interval: str
    checkpoint_location: str
    output_mode: str = "append"
    max_offsets_per_trigger: Optional[int] = None


@dataclass
class SchemaDriftConfig:
    mode: str = "auto_evolve"  # auto_evolve | strict
    alert_on_drift: bool = True
    allowed_changes: List[str] = field(default_factory=lambda: ["add_column"])


@dataclass
class SnapshotAuditConfig:
    enabled: bool = False
    retention_days: int = 90
    partition_by: Optional[str] = None


@dataclass
class ReconciliationConfig:
    enabled: bool = False
    schedule: str = "weekly"
    method: str = "full_snapshot"


@dataclass
class CdcConfig:
    source_type: Optional[str] = None
    topic: Optional[str] = None
    operation_column: str = "op"
    delete_values: List[str] = field(default_factory=lambda: ["d", "delete", "DELETE"])


@dataclass
class TableConfig:
    source_name: str
    database: str
    source_schema: str
    source_table: str
    target_name: str
    enabled: bool
    execution_order: int

    # Legacy/runtime load type used by connector dispatch: full|incremental|stream
    load_type: str
    # Strategy-level intent from PLAN.md: snapshot|incremental|cdc|stream
    load_strategy: str

    primary_key: List[str]
    watermark_column: Optional[str]
    sequence_by_column: Optional[str]
    fetch_size: int
    stored_as_scd_type: int

    stream_config: Optional[StreamConfig]
    snapshot_audit: SnapshotAuditConfig
    reconciliation: ReconciliationConfig
    cdc: CdcConfig

    schema_drift: SchemaDriftConfig
    expectations: List[QualityExpectation]

    silver_mode: str  # batch | streaming
    silver_transformations: List[Dict]
    scd_type: int
    merge_keys: List[str]


class ConfigReader:
    """Reads and parses YAML configuration files for the ingestion framework."""

    def __init__(self, environment: str, base_path: Optional[str] = None):
        self.environment = environment
        self.base_path = base_path or self._detect_base_path()
        self._env_config = self._load_environment()

    def _detect_base_path(self) -> str:
        """Auto-detect base path by looking for config directory."""
        try:
            current = os.path.dirname(os.path.abspath(__file__))
            for _ in range(5):
                if os.path.exists(os.path.join(current, "config")):
                    return current
                current = os.path.dirname(current)
        except NameError:
            pass

        for p in sys.path:
            candidate = os.path.dirname(p) if p.endswith("/src") else p
            if os.path.exists(os.path.join(candidate, "config")):
                return candidate

        return os.getcwd()

    def _load_yaml(self, relative_path: str) -> dict:
        full_path = os.path.join(self.base_path, relative_path)
        with open(full_path, "r") as f:
            return yaml.safe_load(f)

    def _load_environment(self) -> dict:
        return self._load_yaml(f"config/environments/{self.environment}.yml")

    def get_source_config(self, source_name: str) -> dict:
        return self._load_yaml(f"config/sources/{source_name}.yml")["source"]

    def get_all_table_configs(self) -> List[TableConfig]:
        """Load all enabled table configs from nested directory structure."""
        tables: List[TableConfig] = []
        tables_dir = os.path.join(self.base_path, "config", "tables")

        for source_dir in sorted(os.listdir(tables_dir)):
            source_path = os.path.join(tables_dir, source_dir)
            if not os.path.isdir(source_path):
                continue

            for root, dirs, files in os.walk(source_path):
                dirs.sort()
                for table_file in sorted(files):
                    if table_file.startswith("_") or not table_file.endswith(".yml"):
                        continue

                    rel_path = os.path.relpath(
                        os.path.join(root, table_file), self.base_path
                    )
                    config = self._load_yaml(rel_path)
                    table_cfg = self._parse_table_config(config["table"])
                    if table_cfg.enabled:
                        tables.append(table_cfg)

        tables.sort(key=lambda t: t.execution_order)
        return tables

    def get_tables_by_load_type(self, load_type: str) -> List[TableConfig]:
        return [t for t in self.get_all_table_configs() if t.load_type == load_type]

    def get_tables_by_load_strategy(self, load_strategy: str) -> List[TableConfig]:
        return [
            t for t in self.get_all_table_configs() if t.load_strategy == load_strategy
        ]

    def get_gold_models(self) -> List[dict]:
        models: List[dict] = []
        gold_dir = os.path.join(self.base_path, "config", "gold")

        for model_file in sorted(os.listdir(gold_dir)):
            if not model_file.endswith(".yml"):
                continue
            model = self._load_yaml(f"config/gold/{model_file}")
            # backward compatible: prefer gold_model, fallback model
            models.append(model.get("gold_model") or model.get("model"))

        return models

    def get_env_value(self, *keys, default=None):
        env = self._env_config.get("environment", {})
        for key in keys:
            if isinstance(env, dict):
                env = env.get(key)
            else:
                return default
        return env if env is not None else default

    def _resolve_load_strategy(self, load: dict, stream_raw: Optional[dict]) -> str:
        strategy = (load.get("strategy") or "").strip().lower()
        if strategy:
            if strategy in {"snapshot", "incremental", "cdc", "stream"}:
                return strategy
            return "snapshot"

        # Legacy compatibility: infer strategy from load.type
        legacy_type = (load.get("type") or "full").strip().lower()
        if legacy_type == "full":
            return "snapshot"
        if legacy_type == "incremental":
            return "incremental"
        if legacy_type == "stream":
            source_type = (stream_raw or {}).get("source_type", "").lower()
            return "cdc" if source_type == "cdc" else "stream"
        return "snapshot"

    def _resolve_load_type(
        self, load: dict, load_strategy: str, stream_raw: Optional[dict]
    ) -> str:
        legacy_type = (load.get("type") or "").strip().lower()
        if legacy_type in {"full", "incremental", "stream"}:
            return legacy_type

        if load_strategy == "snapshot":
            return "full"
        if load_strategy == "incremental":
            return "incremental"
        if load_strategy in {"cdc", "stream"}:
            return "stream"

        return "stream" if stream_raw else "full"

    def _parse_table_config(self, raw: dict) -> TableConfig:
        load = raw.get("load", {})
        drift = raw.get("schema_drift", {})
        quality = raw.get("quality", {})
        silver = raw.get("silver", {})

        stream_raw = load.get("stream")
        load_strategy = self._resolve_load_strategy(load, stream_raw)
        load_type = self._resolve_load_type(load, load_strategy, stream_raw)

        cdc_raw = load.get("cdc", {})
        snapshot_audit_raw = load.get("snapshot_audit", {})
        reconciliation_raw = load.get("reconciliation", {})

        stream_config = None
        if stream_raw or load_strategy in {"cdc", "stream"}:
            stream_raw = stream_raw or {}
            checkpoint_base = self.get_env_value("checkpoint_base", default="")
            checkpoint_template = stream_raw.get(
                "checkpoint_location",
                "__CHECKPOINT_BASE__/bronze/{target_name}",
            )
            checkpoint = checkpoint_template.replace(
                "__CHECKPOINT_BASE__", checkpoint_base
            ).replace("{target_name}", raw["target_name"])

            env_trigger = self.get_env_value(
                "overrides", "streaming_trigger_interval"
            )
            stream_config = StreamConfig(
                source_type=stream_raw.get(
                    "source_type",
                    cdc_raw.get("source_type", "cdc" if load_strategy == "cdc" else "autoloader"),
                ),
                trigger_interval=env_trigger or stream_raw.get("trigger_interval", "60 seconds"),
                checkpoint_location=checkpoint,
                output_mode=stream_raw.get("output_mode", "append"),
                max_offsets_per_trigger=stream_raw.get("max_offsets_per_trigger"),
            )

        expectations: List[QualityExpectation] = []
        env_quality_override = self.get_env_value("overrides", "default_quality_action")
        for exp in quality.get("expectations", []):
            action = env_quality_override if env_quality_override else exp["action"]
            expectations.append(
                QualityExpectation(
                    name=exp["name"],
                    constraint=exp["constraint"],
                    action=action,
                )
            )

        return TableConfig(
            source_name=raw["source"],
            database=raw.get("database", ""),
            source_schema=raw["source_schema"],
            source_table=raw["source_table"],
            target_name=raw["target_name"],
            enabled=raw.get("enabled", True),
            execution_order=raw.get("execution_order", 999),
            load_type=load_type,
            load_strategy=load_strategy,
            primary_key=load.get("primary_key", []),
            watermark_column=load.get("watermark_column"),
            sequence_by_column=load.get("sequence_by_column") or load.get("watermark_column"),
            fetch_size=load.get("fetch_size", 10000),
            stored_as_scd_type=int(load.get("stored_as_scd_type", 1)),
            stream_config=stream_config,
            snapshot_audit=SnapshotAuditConfig(
                enabled=bool(snapshot_audit_raw.get("enabled", False)),
                retention_days=int(snapshot_audit_raw.get("retention_days", 90)),
                partition_by=snapshot_audit_raw.get("partition_by"),
            ),
            reconciliation=ReconciliationConfig(
                enabled=bool(reconciliation_raw.get("enabled", False)),
                schedule=str(reconciliation_raw.get("schedule", "weekly")),
                method=str(reconciliation_raw.get("method", "full_snapshot")),
            ),
            cdc=CdcConfig(
                source_type=cdc_raw.get("source_type"),
                topic=cdc_raw.get("topic"),
                operation_column=cdc_raw.get("operation_column", "op"),
                delete_values=cdc_raw.get("delete_values", ["d", "delete", "DELETE"]),
            ),
            schema_drift=SchemaDriftConfig(
                mode=drift.get("mode", "auto_evolve"),
                alert_on_drift=drift.get("alert_on_drift", True),
                allowed_changes=drift.get("allowed_changes", ["add_column"]),
            ),
            expectations=expectations,
            silver_mode=silver.get("mode", "batch"),
            silver_transformations=silver.get("transformations", []),
            scd_type=int(silver.get("scd_type", 1)),
            merge_keys=silver.get("merge_keys", load.get("primary_key", [])),
        )
