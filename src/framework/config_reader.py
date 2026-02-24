"""
Config reader module: loads YAML configs, validates against schema,
merges environment overrides, and provides structured config objects
to DLT notebooks.
"""
import yaml
import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional


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
class TableConfig:
    source_name: str
    source_schema: str
    source_table: str
    target_name: str
    enabled: bool
    execution_order: int
    load_type: str  # full | incremental | stream
    primary_key: List[str]
    watermark_column: Optional[str]
    fetch_size: int
    stream_config: Optional[StreamConfig]
    schema_drift: SchemaDriftConfig
    expectations: List[QualityExpectation]
    silver_transformations: List[Dict]
    scd_type: int
    merge_keys: List[str]


class ConfigReader:
    """Reads and parses YAML configuration files for the ingestion framework."""

    def __init__(self, environment: str, base_path: Optional[str] = None):
        """
        Args:
            environment: Target environment name (dev, staging, prod)
            base_path: Root path of the bundle project. If None, auto-detected.
        """
        self.environment = environment
        self.base_path = base_path or self._detect_base_path()
        self._env_config = self._load_environment()

    def _detect_base_path(self) -> str:
        """Auto-detect base path by looking for databricks.yml."""
        current = os.path.dirname(os.path.abspath(__file__))
        # Walk up from src/framework/ to project root
        for _ in range(5):
            if os.path.exists(os.path.join(current, "databricks.yml")):
                return current
            current = os.path.dirname(current)
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def _load_yaml(self, relative_path: str) -> dict:
        """Load a YAML file from the project."""
        full_path = os.path.join(self.base_path, relative_path)
        with open(full_path, "r") as f:
            return yaml.safe_load(f)

    def _load_environment(self) -> dict:
        """Load environment-specific configuration."""
        return self._load_yaml(f"config/environments/{self.environment}.yml")

    def get_source_config(self, source_name: str) -> dict:
        """Load a source system configuration."""
        return self._load_yaml(f"config/sources/{source_name}.yml")["source"]

    def get_all_table_configs(self) -> List[TableConfig]:
        """Load all enabled table configs, sorted by execution_order."""
        tables = []
        tables_dir = os.path.join(self.base_path, "config", "tables")

        for source_dir in sorted(os.listdir(tables_dir)):
            source_path = os.path.join(tables_dir, source_dir)
            if not os.path.isdir(source_path):
                continue

            for table_file in sorted(os.listdir(source_path)):
                if table_file.startswith("_") or not table_file.endswith(".yml"):
                    continue

                config = self._load_yaml(f"config/tables/{source_dir}/{table_file}")
                table_cfg = self._parse_table_config(config["table"])
                if table_cfg.enabled:
                    tables.append(table_cfg)

        tables.sort(key=lambda t: t.execution_order)
        return tables

    def get_tables_by_load_type(self, load_type: str) -> List[TableConfig]:
        """Get tables filtered by load type (full, incremental, stream)."""
        return [t for t in self.get_all_table_configs() if t.load_type == load_type]

    def get_gold_models(self) -> List[dict]:
        """Load all Gold model definitions."""
        models = []
        gold_dir = os.path.join(self.base_path, "config", "gold")

        for model_file in sorted(os.listdir(gold_dir)):
            if model_file.endswith(".yml"):
                model = self._load_yaml(f"config/gold/{model_file}")
                models.append(model["gold_model"])

        return models

    def get_env_value(self, *keys, default=None):
        """Get a nested value from the environment config.

        Example: get_env_value("bronze", "storage_path")
        """
        env = self._env_config.get("environment", {})
        for key in keys:
            if isinstance(env, dict):
                env = env.get(key)
            else:
                return default
        return env if env is not None else default

    def _parse_table_config(self, raw: dict) -> TableConfig:
        """Parse raw YAML dict into a strongly-typed TableConfig."""
        load = raw.get("load", {})
        drift = raw.get("schema_drift", {})
        quality = raw.get("quality", {})
        silver = raw.get("silver", {})
        stream_raw = load.get("stream")

        # Parse stream config
        stream_config = None
        if stream_raw:
            checkpoint_base = self.get_env_value("checkpoint_base", default="")
            checkpoint = stream_raw["checkpoint_location"].replace(
                "__CHECKPOINT_BASE__", checkpoint_base
            )
            # Apply environment override for trigger interval
            env_trigger = self.get_env_value(
                "overrides", "streaming_trigger_interval"
            )
            stream_config = StreamConfig(
                source_type=stream_raw["source_type"],
                trigger_interval=env_trigger or stream_raw["trigger_interval"],
                checkpoint_location=checkpoint,
                output_mode=stream_raw.get("output_mode", "append"),
                max_offsets_per_trigger=stream_raw.get("max_offsets_per_trigger"),
            )

        # Parse quality expectations with env override
        expectations = []
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
            source_schema=raw["source_schema"],
            source_table=raw["source_table"],
            target_name=raw["target_name"],
            enabled=raw.get("enabled", True),
            execution_order=raw.get("execution_order", 999),
            load_type=load.get("type", "full"),
            primary_key=load.get("primary_key", []),
            watermark_column=load.get("watermark_column"),
            fetch_size=load.get("fetch_size", 10000),
            stream_config=stream_config,
            schema_drift=SchemaDriftConfig(
                mode=drift.get("mode", "auto_evolve"),
                alert_on_drift=drift.get("alert_on_drift", True),
                allowed_changes=drift.get("allowed_changes", ["add_column"]),
            ),
            expectations=expectations,
            silver_transformations=silver.get("transformations", []),
            scd_type=silver.get("scd_type", 1),
            merge_keys=silver.get("merge_keys", load.get("primary_key", [])),
        )
