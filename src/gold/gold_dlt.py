# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Star Schema and Serving Models
# MAGIC
# MAGIC - dim_date is generated automatically
# MAGIC - models are loaded from config/gold/*.yml
# MAGIC - supports SCD1 materialized views and optional SCD2 snapshot flows

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
import inspect
import os
import re
import sys

try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _workspace_root = "/Workspace" + str(_nb_path).rsplit("/src/", 1)[0]
    sys.path.insert(0, os.path.join(_workspace_root, "src"))
except Exception:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath("."))), "src"))

from framework.config_reader import ConfigReader
from framework.quality_engine import apply_expectations_from_raw

# COMMAND ----------

environment = spark.conf.get("environment", "dev")
catalog = spark.conf.get("catalog", "main")
silver_schema = spark.conf.get("silver_schema", "dev_silver")

auto_cdc_snapshot_supported = hasattr(dp, "create_auto_cdc_from_snapshot_flow")
streaming_table_supported = hasattr(dp, "create_streaming_table")

config = ConfigReader(environment)

# COMMAND ----------


@dp.materialized_view(
    name="dim_date",
    comment="Date dimension - auto-generated (2010-2030)",
    table_properties={"quality": "gold"},
)
def dim_date():
    return spark.sql(
        """
        WITH date_spine AS (
            SELECT explode(
                sequence(DATE'2010-01-01', DATE'2030-12-31', INTERVAL 1 DAY)
            ) AS date_key
        )
        SELECT
            date_key,
            YEAR(date_key) AS year,
            QUARTER(date_key) AS quarter,
            MONTH(date_key) AS month,
            DAY(date_key) AS day,
            DAYOFWEEK(date_key) AS day_of_week,
            WEEKOFYEAR(date_key) AS week_of_year,
            DATE_FORMAT(date_key, 'MMMM') AS month_name,
            DATE_FORMAT(date_key, 'MMM') AS month_short,
            DATE_FORMAT(date_key, 'EEEE') AS day_name,
            CASE WHEN DAYOFWEEK(date_key) IN (1, 7) THEN true ELSE false END AS is_weekend,
            CONCAT('Q', QUARTER(date_key)) AS quarter_name,
            CONCAT(YEAR(date_key), '-Q', QUARTER(date_key)) AS year_quarter,
            DATE_FORMAT(date_key, 'yyyy-MM') AS year_month,
            current_timestamp() AS etl_timestamp
        FROM date_spine
        """
    )


@dp.materialized_view(
    name="pipeline_quality_kpis",
    comment="Basic quality and volume KPIs for serving layer",
    table_properties={"quality": "gold", "model_type": "monitoring"},
)
def pipeline_quality_kpis():
    return spark.sql(
        f"""
        SELECT
          current_timestamp() AS collected_at,
          '{environment}' AS environment,
          '{catalog}' AS catalog_name,
          (SELECT COUNT(*) FROM {catalog}.{silver_schema}.region) AS silver_region_rows,
          (SELECT COUNT(*) FROM {catalog}.{silver_schema}.nation) AS silver_nation_rows,
          (SELECT COUNT(*) FROM {catalog}.{silver_schema}.customer) AS silver_customer_rows,
          (SELECT COUNT(*) FROM {catalog}.{silver_schema}.orders) AS silver_orders_rows,
          (SELECT COUNT(*) FROM {catalog}.{silver_schema}.lineitem) AS silver_lineitem_rows
        """
    )


# COMMAND ----------


def qualify_silver_tables(sql: str, source_tables: list, catalog_name: str, silver_sch: str) -> str:
    """Replace unqualified Silver table names with catalog.schema.table."""
    for src in source_tables:
        table_name = src.split(".")[-1]
        fqn = f"{catalog_name}.{silver_sch}.{table_name}"
        sql = re.sub(rf"\b{table_name}\b", fqn, sql)
    return sql



def normalize_model(model_cfg: dict) -> dict:
    cfg = dict(model_cfg)
    cfg.setdefault("type", "materialized_view")
    cfg.setdefault("scd_type", 1)
    cfg.setdefault("depends_on", [])
    cfg.setdefault("source_tables", [])
    cfg.setdefault("quality", {})
    return cfg



def _filter_supported_kwargs(func, kwargs: dict):
    """
    Keep only kwargs supported by the runtime function signature.
    Some DLT runtimes do not accept optional args like `name` / `comment`
    for snapshot AUTO CDC APIs.
    """
    try:
        signature = inspect.signature(func)
        params = signature.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs
        return {k: v for k, v in kwargs.items() if k in params}
    except (TypeError, ValueError):
        filtered = dict(kwargs)
        filtered.pop("name", None)
        filtered.pop("comment", None)
        return filtered



def order_models_by_dependencies(models: list) -> list:
    """Topological ordering using optional depends_on entries."""
    by_name = {m["name"]: m for m in models}
    visiting = set()
    visited = set()
    ordered = []

    def visit(name: str):
        if name in visited:
            return
        if name in visiting:
            raise ValueError(f"Circular gold model dependency detected: {name}")
        visiting.add(name)

        model = by_name[name]
        for dep in model.get("depends_on", []):
            if dep in by_name:
                visit(dep)

        visiting.remove(name)
        visited.add(name)
        ordered.append(model)

    for n in by_name:
        visit(n)

    return ordered



def create_gold_mv(model_cfg, qualified_sql: str):
    expectations = apply_expectations_from_raw(
        model_cfg.get("quality", {}).get("expectations", [])
    )

    @dp.materialized_view(
        name=model_cfg["name"],
        comment=model_cfg.get("description", ""),
        table_properties={
            "quality": "gold",
            "model_type": str(model_cfg.get("type", "materialized_view")),
            "scd_type": str(model_cfg.get("scd_type", 1)),
        },
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _gold_mv():
        return spark.sql(qualified_sql)



def create_gold_scd2(model_cfg, qualified_sql: str):
    natural_keys = model_cfg.get("natural_key") or []
    if not natural_keys:
        print(f"  [Gold][WARN] {model_cfg['name']} has scd_type=2 but no natural_key; fallback to MV")
        create_gold_mv(model_cfg, qualified_sql)
        return

    if not (auto_cdc_snapshot_supported and streaming_table_supported):
        print(f"  [Gold][WARN] AUTO CDC snapshot API unavailable; fallback to MV for {model_cfg['name']}")
        create_gold_mv(model_cfg, qualified_sql)
        return

    expectations = apply_expectations_from_raw(
        model_cfg.get("quality", {}).get("expectations", [])
    )
    source_view_name = f"gold_snapshot_{model_cfg['name']}"

    @dp.temporary_view(name=source_view_name)
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _scd2_snapshot_source():
        return spark.sql(qualified_sql)

    dp.create_streaming_table(
        name=model_cfg["name"],
        comment=model_cfg.get("description", ""),
        table_properties={
            "quality": "gold",
            "model_type": str(model_cfg.get("type", "dimension")),
            "scd_type": "2",
        },
    )

    flow_kwargs = {
        "target": model_cfg["name"],
        "source": source_view_name,
        "keys": natural_keys,
        "stored_as_scd_type": 2,
        "name": f"gold_scd2_{model_cfg['name']}",
        "comment": f"Gold SCD2 snapshot flow for {model_cfg['name']}",
    }

    track_history_columns = model_cfg.get("track_history_columns")
    if track_history_columns:
        flow_kwargs["track_history_column_list"] = track_history_columns

    dp.create_auto_cdc_from_snapshot_flow(
        **_filter_supported_kwargs(dp.create_auto_cdc_from_snapshot_flow, flow_kwargs)
    )


# COMMAND ----------

raw_models = config.get_gold_models()
normalized = [normalize_model(m) for m in raw_models if m and m.get("name")]

# dim_date and pipeline_quality_kpis are created in code above
models = [
    m
    for m in normalized
    if m["name"] not in {"dim_date", "pipeline_quality_kpis"}
]
models = order_models_by_dependencies(models)

print(f"[Gold] Loading {len(models)} configured models (env: {environment})")
print(
    "[Gold] API support - "
    f"streaming_table={streaming_table_supported}, auto_cdc_snapshot={auto_cdc_snapshot_supported}"
)

for model_cfg in models:
    qualified_sql = qualify_silver_tables(
        model_cfg["sql"],
        model_cfg.get("source_tables", []),
        catalog,
        silver_schema,
    )

    if int(model_cfg.get("scd_type", 1)) == 2:
        create_gold_scd2(model_cfg, qualified_sql)
        print(f"  [SCD2] {model_cfg['name']}")
    else:
        create_gold_mv(model_cfg, qualified_sql)
        print(f"  [MV] {model_cfg['name']}")
