# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Star Schema Models
# MAGIC
# MAGIC Creates Gold dimension and fact tables from YAML model definitions
# MAGIC using Spark Declarative Pipelines (pyspark.pipelines).
# MAGIC dim_date is auto-generated; all others are defined in config/gold/*.yml

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
import sys
import os

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

config = ConfigReader(environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date (Auto-Generated)

# COMMAND ----------

@dp.materialized_view(
    name="dim_date",
    comment="Date dimension - auto-generated (2010-2030)",
    table_properties={"quality": "gold"},
)
def dim_date():
    return spark.sql("""
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
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Gold Models from YAML Config

# COMMAND ----------

def qualify_silver_tables(sql: str, source_tables: list, catalog: str, silver_schema: str) -> str:
    """Replace unqualified Silver table names with fully qualified catalog.schema.table."""
    import re
    for src in source_tables:
        # source_tables entries like "silver.customer" -> extract table name
        table_name = src.split(".")[-1]
        fqn = f"{catalog}.{silver_schema}.{table_name}"
        # Replace standalone table name references (word boundary)
        sql = re.sub(rf'\b{table_name}\b', fqn, sql)
    return sql


def create_gold_model(model_cfg):
    """Create a Gold materialized view from a YAML model definition."""
    expectations = apply_expectations_from_raw(
        model_cfg.get("quality", {}).get("expectations", [])
    )
    # Qualify Silver table references in SQL with full catalog.schema path
    qualified_sql = qualify_silver_tables(
        model_cfg["sql"],
        model_cfg.get("source_tables", []),
        catalog,
        silver_schema,
    )

    @dp.materialized_view(
        name=model_cfg["name"],
        comment=model_cfg.get("description", ""),
        table_properties={"quality": "gold"},
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def gold_table():
        return spark.sql(qualified_sql)

    return gold_table


# COMMAND ----------

# Generate all Gold models from YAML
gold_models = config.get_gold_models()
print(f"[Gold] Loading {len(gold_models)} models from config (env: {environment})")

for model_cfg in gold_models:
    if model_cfg["name"] != "dim_date":  # dim_date handled above
        create_gold_model(model_cfg)
        print(f"  [MATERIALIZED_VIEW] {model_cfg['name']}")
