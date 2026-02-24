# Metadata-Driven Ingestion Framework

An enterprise-grade, configuration-driven data ingestion and transformation framework for **Databricks**, built with **Databricks Asset Bundles (DAB)** and **Delta Live Tables (DLT)**.

All pipeline logic is defined in YAML — git-versioned, PR-reviewable, and rollback-friendly. No more hardcoded ETL scripts or unversioned database control tables.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration Guide](#configuration-guide)
  - [Sources](#1-source-configuration)
  - [Tables](#2-table-configuration)
  - [Gold Models](#3-gold-model-configuration)
  - [Environments](#4-environment-configuration)
- [Framework Modules](#framework-modules)
  - [ConfigReader](#configreader)
  - [SourceConnectorFactory](#sourceconnectorfactory)
  - [QualityEngine](#qualityengine)
  - [SchemaDriftHandler](#schemadrifthandler)
  - [WatermarkManager](#watermarkmanager)
- [DLT Pipeline Notebooks](#dlt-pipeline-notebooks)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Load Patterns](#load-patterns)
  - [Full Load](#full-load)
  - [Incremental Load](#incremental-load)
  - [Streaming](#streaming)
- [Data Quality](#data-quality)
- [Schema Drift Handling](#schema-drift-handling)
- [SCD (Slowly Changing Dimensions)](#scd-slowly-changing-dimensions)
- [Deployment](#deployment)
  - [Bundle Commands](#bundle-commands)
  - [CI/CD Pipeline](#cicd-pipeline)
- [Adding a New Table](#adding-a-new-table)
- [Adding a New Source System](#adding-a-new-source-system)
- [Monitoring & Health Checks](#monitoring--health-checks)
- [Deployed Resources](#deployed-resources)
- [Secrets Configuration](#secrets-configuration)
- [Cluster & Performance](#cluster--performance)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        YAML Configuration                          │
│  config/sources/  │  config/tables/  │  config/gold/  │  config/env│
└────────┬──────────┴────────┬─────────┴───────┬────────┴─────┬──────┘
         │                   │                 │              │
         ▼                   ▼                 ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Framework Engine (Python)                       │
│  ConfigReader │ SourceConnectors │ QualityEngine │ SchemaDrift │ WM │
└────────┬──────┴────────┬─────────┴───────┬───────┴──────┬──────────┘
         │               │                 │              │
         ▼               ▼                 ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Delta Live Tables (DLT)                          │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                      │
│  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │                      │
│  │ Raw Data │    │ Cleansed │    │Star Schema│                      │
│  │          │    │  SCD 1/2 │    │ Facts/Dims│                      │
│  └──────────┘    └──────────┘    └──────────┘                      │
│   ADLS Gen2       Unity Catalog    Unity Catalog                    │
│   External         Managed          Managed                         │
└─────────────────────────────────────────────────────────────────────┘
         │               │                 │
         ▼               ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Orchestrator Job (Bronze → Silver → Gold)              │
│              Monitoring Job (Watermarks, Row Counts)                │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Sources Supported

| Source Type | Load Modes | Connector |
|-------------|-----------|-----------|
| SQL Server (JDBC) | Full, Incremental | `spark.read.format("jdbc")` |
| Azure Event Hub | Streaming | `spark.readStream.format("eventhubs")` |
| Apache Kafka | Streaming | `spark.readStream.format("kafka")` |
| CDC (Debezium) | Streaming | Kafka Connect + Debezium topics |
| Auto Loader | Batch, Streaming | `spark.readStream.format("cloudFiles")` |

### Storage Strategy

| Layer | Storage | Managed By |
|-------|---------|------------|
| Bronze | ADLS Gen2 (`abfss://datalake@daqprod.dfs.core.windows.net/bronze/`) | External tables |
| Silver | Unity Catalog | Managed tables |
| Gold | Unity Catalog | Managed tables |

---

## Project Structure

```
metadata-driven-ingestion/
│
├── databricks.yml                              # Bundle root config (targets, variables)
│
├── config/                                     # All metadata — YAML only
│   ├── sources/
│   │   └── adventureworks.yml                  # Source system connection config
│   ├── tables/
│   │   └── adventureworks/
│   │       ├── sales_customer.yml              # Per-table: load, quality, drift, transforms
│   │       ├── sales_salesorderheader.yml
│   │       ├── sales_salesorderdetail.yml
│   │       ├── sales_salesperson.yml
│   │       ├── sales_salesterritory.yml
│   │       ├── sales_currency.yml
│   │       ├── sales_currencyrate.yml
│   │       ├── sales_specialoffer.yml
│   │       ├── sales_salesreason.yml
│   │       ├── sales_store.yml
│   │       ├── sales_salespersonquotahistory.yml
│   │       └── sales_salesorderheadersalesreason.yml
│   ├── gold/
│   │   ├── fact_sales.yml                      # Star schema model definitions
│   │   ├── dim_customer.yml
│   │   ├── dim_salesperson.yml
│   │   ├── dim_territory.yml
│   │   ├── dim_currency.yml
│   │   ├── dim_promotion.yml
│   │   └── dim_sales_reason.yml
│   └── environments/
│       ├── dev.yml                             # Dev overrides (catalog, cluster, quality)
│       ├── staging.yml
│       └── prod.yml
│
├── resources/                                  # Databricks resource definitions
│   ├── bronze_dlt_pipeline.yml                 # DLT pipeline for Bronze layer
│   ├── silver_dlt_pipeline.yml                 # DLT pipeline for Silver layer
│   ├── gold_dlt_pipeline.yml                   # DLT pipeline for Gold layer
│   ├── orchestrator_job.yml                    # Workflow: Bronze → Silver → Gold
│   └── monitoring_job.yml                      # Health check job (every 30 min)
│
├── src/                                        # Python source code
│   ├── framework/                              # Core engine
│   │   ├── config_reader.py                    # YAML loader, parser, env merger
│   │   ├── source_connectors.py                # Multi-source DataFrame factory
│   │   ├── quality_engine.py                   # YAML → DLT Expectations
│   │   ├── schema_drift.py                     # auto_evolve / strict detection
│   │   └── watermark_manager.py                # Delta-based incremental tracking
│   ├── bronze/
│   │   └── bronze_dlt.py                       # Dynamic Bronze DLT table generation
│   ├── silver/
│   │   └── silver_dlt.py                       # Silver transforms + SCD Type 1/2
│   ├── gold/
│   │   └── gold_dlt.py                         # Star schema Gold models
│   └── utils/
│       └── health_check.py                     # Monitoring notebook
│
├── .github/workflows/
│   └── deploy.yml                              # CI/CD: validate → staging → prod
│
└── requirements.txt                            # PyYAML>=6.0
```

---

## Prerequisites

1. **Databricks CLI v0.200+** (new CLI with bundle support)

   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
   databricks --version  # Should show v0.200+
   ```

2. **Databricks workspace** with Unity Catalog enabled

3. **Authentication** configured:

   ```bash
   # Option 1: Personal Access Token
   databricks configure --host https://your-workspace.azuredatabricks.net --token

   # Option 2: ~/.databrickscfg already exists
   cat ~/.databrickscfg
   ```

4. **Secret Scope** `adls-secrets` with required keys (see [Secrets Configuration](#secrets-configuration))

5. **ADLS Gen2** storage account accessible from workspace

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yasarkocyigit/daq-databricks-dab.git
cd daq-databricks-dab

# 2. Validate the bundle
databricks bundle validate

# 3. Deploy to dev
databricks bundle deploy -t dev

# 4. Run the orchestrator (Bronze → Silver → Gold)
databricks bundle run orchestrator -t dev

# 5. Check deployed resources
databricks bundle summary -t dev
```

---

## Configuration Guide

### 1. Source Configuration

**File:** `config/sources/{source_name}.yml`

Defines how to connect to a source system. All credentials are referenced via Databricks secret scopes — never hardcoded.

```yaml
source:
  name: adventureworks                          # Unique identifier
  type: jdbc                                    # jdbc | autoloader
  description: "AdventureWorks OLTP on SQL Server"

  connection:
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    host_secret_scope: "adls-secrets"           # Databricks secret scope name
    host_secret_key: "sqlserver-host"            # Key within the scope
    port: 1433
    database: "AdventureWorks2019"
    username_secret_key: "sqlserver-username"
    password_secret_key: "sqlserver-password"

    jdbc_options:                                # Passed directly to JDBC driver
      encrypt: "true"
      trustServerCertificate: "true"
      loginTimeout: 30
      queryTimeout: 7200
```

**For streaming sources**, add the relevant section:

```yaml
  # Azure Event Hub
  eventhub:
    namespace_secret_key: "eventhub-namespace"
    connection_string_secret_key: "eventhub-connection-string"
    consumer_group: "$Default"
    starting_position: "earliest"               # earliest | latest

  # Apache Kafka
  kafka:
    bootstrap_servers_secret_key: "kafka-brokers"
    security_protocol: "SASL_SSL"
    sasl_mechanism: "PLAIN"
    sasl_username_secret_key: "kafka-username"
    sasl_password_secret_key: "kafka-password"

  # Auto Loader (file-based)
  autoloader:
    path: "abfss://landing@daqprod.dfs.core.windows.net/incoming/"
    format: "json"                              # json | csv | parquet | avro
    schema_location: "abfss://datalake@daqprod.dfs.core.windows.net/_schemas/"
```

---

### 2. Table Configuration

**File:** `config/tables/{source_name}/{table_name}.yml`

Each table has its own YAML file — controls load type, data quality, schema drift policy, Silver transformations, and SCD type.

```yaml
table:
  source: adventureworks                        # References source config
  source_schema: Sales
  source_table: Customer
  target_name: customer                         # Name in Bronze/Silver/Gold
  enabled: true                                 # Set false to skip
  execution_order: 10                           # Lower = earlier in pipeline

  # ── Load Configuration ──────────────────────────────────────────
  load:
    type: incremental                           # full | incremental | stream
    watermark_column: ModifiedDate              # For incremental: tracks last loaded value
    primary_key: [CustomerID]                   # Used for merge/upsert operations
    fetch_size: 10000                           # JDBC batch size

    # For streaming tables only:
    # stream:
    #   source_type: cdc                        # eventhub | kafka | cdc | autoloader
    #   trigger_interval: "10 seconds"
    #   checkpoint_location: "__CHECKPOINT_BASE__/customer"
    #   output_mode: append
    #   max_offsets_per_trigger: 10000

  # ── Schema Drift Policy ─────────────────────────────────────────
  schema_drift:
    mode: auto_evolve                           # auto_evolve | strict
    alert_on_drift: true
    allowed_changes: [add_column]               # add_column | drop_column | type_change

  # ── Data Quality Rules ──────────────────────────────────────────
  quality:
    expectations:
      - name: "customer_id_not_null"
        constraint: "CustomerID IS NOT NULL"
        action: fail                            # fail | warn | drop
      - name: "valid_territory"
        constraint: "TerritoryID IS NOT NULL AND TerritoryID > 0"
        action: warn
      - name: "valid_account_number"
        constraint: "AccountNumber IS NOT NULL AND LENGTH(AccountNumber) > 0"
        action: drop                            # Quarantine bad rows

  # ── Silver Layer Transformations ────────────────────────────────
  silver:
    transformations:
      - type: add_column
        name: customer_type
        expression: "CASE WHEN StoreID IS NULL THEN 'Individual' ELSE 'Store' END"
      - type: add_column
        name: source_system
        expression: "'AdventureWorks'"
      - type: trim_strings
        columns: all                            # Trim all string columns
      - type: null_standardize
        values: ["NULL", "null", "N/A", "n/a", ""]

    scd_type: 1                                 # 1 = overwrite, 2 = history tracking
    merge_keys: [CustomerID]

  # ── Schedule ────────────────────────────────────────────────────
  schedule:
    cron: "0 0 6 * * ?"                         # Daily at 6 AM
    timezone: "America/New_York"
```

#### Supported Silver Transformations

| Type | Parameters | Description |
|------|-----------|-------------|
| `add_column` | `name`, `expression` | Add computed column using SQL expression |
| `trim_strings` | `columns: all \| [list]` | Remove leading/trailing whitespace |
| `null_standardize` | `values: [list]` | Replace specified values with NULL |
| `rename_column` | `from`, `to` | Rename a column |
| `cast` | `column`, `target_type` | Cast column to new data type |
| `drop_columns` | `columns: [list]` | Remove columns |

---

### 3. Gold Model Configuration

**File:** `config/gold/{model_name}.yml`

Defines star schema models (facts and dimensions) using SQL with `LIVE.` references to Silver DLT tables.

```yaml
gold_model:
  name: fact_sales
  type: materialized_view                       # materialized_view | streaming_table
  description: "Sales transactions at order line level"

  source_tables:                                # Documentation of dependencies
    - silver.salesorderheader
    - silver.salesorderdetail

  sql: |                                        # SQL with LIVE.{table} references
    SELECT
      d.SalesOrderDetailID AS sales_key,
      d.SalesOrderID AS order_id,
      h.CustomerID AS customer_key,
      h.SalesPersonID AS salesperson_key,
      h.TerritoryID AS territory_key,
      d.SpecialOfferID AS promotion_key,
      CAST(h.OrderDate AS DATE) AS order_date_key,
      d.OrderQty AS quantity,
      d.UnitPrice AS unit_price,
      d.LineTotal AS line_total,
      h.TotalDue AS order_total,
      current_timestamp() AS etl_timestamp
    FROM LIVE.salesorderdetail d
    INNER JOIN LIVE.salesorderheader h
      ON d.SalesOrderID = h.SalesOrderID

  quality:
    expectations:
      - name: "valid_quantity"
        constraint: "quantity > 0"
        action: warn
```

> **Note:** `dim_date` is auto-generated in `gold_dlt.py` (2010–2030) and does not need a YAML config.

---

### 4. Environment Configuration

**File:** `config/environments/{env}.yml`

Override catalog, storage paths, cluster sizing, and quality behavior per environment.

```yaml
environment:
  name: prod
  catalog: main

  bronze:
    storage_path: "abfss://datalake@daqprod.dfs.core.windows.net/bronze"
    schema: bronze_raw

  silver:
    catalog: main
    schema: silver_data

  gold:
    catalog: main
    schema: gold_analytics

  checkpoint_base: "abfss://datalake@daqprod.dfs.core.windows.net/_checkpoints"

  cluster:
    num_workers: 4
    node_type_id: "Standard_DS4_v2"
    spark_version: "14.3.x-scala2.12"
    autoscale:
      min_workers: 2
      max_workers: 8

  overrides:
    default_quality_action: fail                # Override all quality actions
    streaming_trigger_interval: "10 seconds"
```

#### Environment Comparison

| Setting | Dev | Staging | Prod |
|---------|-----|---------|------|
| Catalog | `dev_catalog` | `staging_catalog` | `main` |
| Workers | 1 | 2 | 4 (autoscale 2–8) |
| Node Type | DS3_v2 | DS3_v2 | DS4_v2 |
| Quality Action | `warn` | `warn` | `fail` |
| Stream Trigger | 60 seconds | 30 seconds | 10 seconds |

---

## Framework Modules

### ConfigReader

**File:** `src/framework/config_reader.py`

Central configuration engine. Loads YAML, parses into typed dataclasses, applies environment overrides.

```python
from framework.config_reader import ConfigReader

config = ConfigReader(environment="prod")

# Get all enabled tables sorted by execution_order
tables = config.get_all_table_configs()

# Filter by load type
incremental_tables = config.get_tables_by_load_type("incremental")

# Get source connection details
source = config.get_source_config("adventureworks")

# Get Gold models
models = config.get_gold_models()

# Get nested environment value
catalog = config.get_env_value("catalog", default="main")
```

**Key Dataclasses:**

| Class | Fields |
|-------|--------|
| `TableConfig` | source_name, source_schema, source_table, target_name, enabled, execution_order, load_type, primary_key, watermark_column, fetch_size, stream_config, schema_drift, expectations, silver_transformations, scd_type, merge_keys |
| `QualityExpectation` | name, constraint, action |
| `StreamConfig` | source_type, trigger_interval, checkpoint_location, output_mode, max_offsets_per_trigger |
| `SchemaDriftConfig` | mode, alert_on_drift, allowed_changes |

---

### SourceConnectorFactory

**File:** `src/framework/source_connectors.py`

Factory pattern that returns the correct Spark DataFrame (batch or streaming) based on source type and load configuration.

```python
from framework.source_connectors import SourceConnectorFactory

factory = SourceConnectorFactory(spark, config_reader)
df = factory.get_dataframe(table_config)  # Returns batch or streaming DF
```

**Routing Logic:**

```
table_config.load_type == "stream"
  ├── stream_config.source_type == "autoloader"  → cloudFiles readStream
  ├── stream_config.source_type == "eventhub"    → Event Hub readStream
  ├── stream_config.source_type == "kafka"       → Kafka readStream
  └── stream_config.source_type == "cdc"         → Debezium/Kafka readStream

table_config.load_type in ("full", "incremental")
  ├── source.type == "jdbc"       → JDBC batch read (with watermark filter)
  └── source.type == "autoloader" → File batch read
```

**JDBC Features:**
- Credentials resolved from Databricks secret scope at runtime
- Watermark-based filtering for incremental loads
- Configurable fetch size for performance tuning
- Custom JDBC options (encrypt, timeouts)

---

### QualityEngine

**File:** `src/framework/quality_engine.py`

Converts YAML quality rules into DLT Expectation dictionaries.

```python
from framework.quality_engine import apply_expectations

expectations = apply_expectations(table_config)
# Returns: {"warn": {...}, "drop": {...}, "fail": {...}}

# Used in DLT table definitions:
@dlt.expect_all(expectations["warn"])            # Log but keep rows
@dlt.expect_all_or_drop(expectations["drop"])    # Remove bad rows
@dlt.expect_all_or_fail(expectations["fail"])    # Fail pipeline
```

| Action | DLT Decorator | Behavior |
|--------|--------------|----------|
| `warn` | `@dlt.expect_all()` | Log violation, keep all rows |
| `drop` | `@dlt.expect_all_or_drop()` | Remove non-compliant rows |
| `fail` | `@dlt.expect_all_or_fail()` | Stop pipeline on violation |

---

### SchemaDriftHandler

**File:** `src/framework/schema_drift.py`

Detects schema changes between incoming data and existing tables.

```python
from framework.schema_drift import SchemaDriftHandler

handler = SchemaDriftHandler(spark)
df, drift_detected, details = handler.check_and_handle(
    incoming_df, "catalog.schema.table", table_config
)
```

**Detection:**
- New columns (source added fields)
- Dropped columns (source removed fields)
- Type changes (column data type mismatch)
- Ignores framework metadata columns (`_ingestion_timestamp`, etc.)

**Modes:**

| Mode | Behavior |
|------|----------|
| `auto_evolve` | Allow changes, log and alert. Delta `mergeSchema` handles column additions. |
| `strict` | Only allow changes listed in `allowed_changes`. Raises `SchemaDriftError` otherwise. |

---

### WatermarkManager

**File:** `src/framework/watermark_manager.py`

Tracks incremental load progress in a Delta table.

```python
from framework.watermark_manager import WatermarkManager

wm = WatermarkManager(spark, catalog="main")

# Get last loaded watermark
last_value = wm.get_watermark("customer")  # Returns "2024-01-15 08:30:00"

# Update after successful load
wm.update_watermark("customer", "ModifiedDate", "2024-02-01 12:00:00", row_count=1500)

# Get max value from DataFrame
max_val = wm.get_max_watermark_from_df(df, "ModifiedDate")
```

**Storage:** `{catalog}.framework.watermarks` Delta table

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | STRING | Target table name |
| `watermark_column` | STRING | Column being tracked |
| `watermark_value` | STRING | Last loaded value |
| `row_count` | LONG | Rows loaded in last batch |
| `updated_at` | TIMESTAMP | When watermark was updated |

---

## DLT Pipeline Notebooks

### Bronze Layer

**File:** `src/bronze/bronze_dlt.py`

Dynamically creates one DLT table per enabled table config. Each table gets:

- **Prefix:** `bronze_` (e.g., `bronze_customer`)
- **External path:** ADLS Gen2 (`${bronze_storage_path}/{target_name}`)
- **Metadata columns:** `_ingestion_timestamp`, `_source_system`, `_source_schema`, `_source_table`, `_load_type`
- **Quality expectations** from YAML
- **Schema auto-merge** if `auto_evolve` mode

**How it works:**

```
For each table in config:
  1. Read YAML config (load type, source, quality rules)
  2. Create SourceConnector (JDBC, Kafka, etc.)
  3. Generate DLT table with @dlt.table decorator
  4. Apply @dlt.expect_all decorators for quality
  5. Add metadata columns to DataFrame
  6. Register as Bronze DLT table
```

---

### Silver Layer

**File:** `src/silver/silver_dlt.py`

Reads from Bronze DLT tables, applies YAML-defined transformations, handles SCD patterns.

**Processing Logic per Table:**

```
load_type == "stream"  → create_silver_streaming_table()
                           Uses dlt.readStream() from Bronze

scd_type == 2          → create_silver_scd2_table()
                           Uses dlt.apply_changes() for history tracking

scd_type == 1          → create_silver_scd1_table()
                           Uses dlt.read() with overwrite semantics
```

**All Silver tables have:**
- Change Data Feed enabled (`delta.enableChangeDataFeed = true`)
- Auto-optimization (`pipelines.autoOptimize.managed = true`)
- Metadata columns: `_silver_timestamp`, `_is_current`
- Quality expectations from YAML

---

### Gold Layer

**File:** `src/gold/gold_dlt.py`

Creates star schema analytics models.

**`dim_date`** — auto-generated covering 2010–2030 with:
- year, quarter, month, day, day_of_week, week_of_year
- month_name, month_short, day_name
- is_weekend, quarter_name, year_quarter, year_month

**All other models** — loaded from `config/gold/*.yml`:
- SQL executed with `LIVE.{table}` references to Silver tables
- Quality expectations applied
- Materialized views for BI consumption

---

## Load Patterns

### Full Load

Reads the entire source table on every run. Best for small dimension tables.

```yaml
load:
  type: full
  primary_key: [CurrencyCode]
```

**Flow:** `SELECT * FROM Source` → overwrite Bronze table

### Incremental Load

Reads only new/changed records since last watermark. Best for large transaction tables.

```yaml
load:
  type: incremental
  watermark_column: ModifiedDate
  primary_key: [SalesOrderID]
```

**Flow:**
1. Read last watermark from `{catalog}.framework.watermarks`
2. `SELECT * FROM Source WHERE ModifiedDate > '{last_watermark}'`
3. Append to Bronze table
4. Update watermark with new max value

### Streaming

Continuous ingestion from event sources. Best for real-time data.

```yaml
load:
  type: stream
  primary_key: [EventID]
  stream:
    source_type: eventhub          # eventhub | kafka | cdc | autoloader
    trigger_interval: "10 seconds"
    checkpoint_location: "__CHECKPOINT_BASE__/events"
    output_mode: append
    max_offsets_per_trigger: 10000
```

**Flow:** readStream → continuous processing → append to Bronze streaming table

---

## Data Quality

Quality rules are defined per table in YAML and enforced at every layer via DLT Expectations.

### Expectation Actions

| Action | Behavior | When to Use |
|--------|----------|-------------|
| `fail` | **Pipeline stops.** No data written. | Primary keys, critical business rules |
| `drop` | **Bad rows removed.** Good rows continue. | Data cleansing, optional fields |
| `warn` | **All rows kept.** Violations logged in DLT event log. | Monitoring, soft validations |

### Environment Override

In `config/environments/dev.yml`:

```yaml
overrides:
  default_quality_action: warn    # Never fail in dev, just warn
```

In `config/environments/prod.yml`:

```yaml
overrides:
  default_quality_action: fail    # Strict enforcement in production
```

### Viewing Quality Metrics

DLT automatically tracks expectation results. View in Databricks UI:
- Pipeline → Event Log → Data Quality tab
- Shows pass/fail rates per expectation per table

---

## Schema Drift Handling

### auto_evolve Mode

```yaml
schema_drift:
  mode: auto_evolve
  alert_on_drift: true
```

- New columns automatically added via `mergeSchema`
- All changes logged
- Alert sent if `alert_on_drift: true`
- Pipeline continues

### strict Mode

```yaml
schema_drift:
  mode: strict
  alert_on_drift: true
  allowed_changes: [add_column]    # Only new columns allowed
```

- Only changes in `allowed_changes` are permitted
- Unauthorized changes raise `SchemaDriftError`
- Pipeline fails until schema change is approved (update YAML config)

### Allowed Change Types

| Change | Description |
|--------|-------------|
| `add_column` | Source added a new column |
| `drop_column` | Source removed a column |
| `type_change` | Column data type changed |

---

## SCD (Slowly Changing Dimensions)

### Type 1 — Overwrite

```yaml
silver:
  scd_type: 1
  merge_keys: [CustomerID]
```

- Current values only, no history
- Old values overwritten on update
- Simplest approach for non-historical dimensions

### Type 2 — History Tracking

```yaml
silver:
  scd_type: 2
  merge_keys: [BusinessEntityID]
```

- Full history preserved
- Uses DLT `apply_changes()` with `stored_as_scd_type=2`
- Automatically manages `__START_AT`, `__END_AT`, and `__IS_CURRENT` columns
- Sequence tracked by `watermark_column` or `_ingestion_timestamp`

---

## Deployment

### Bundle Commands

```bash
# Validate YAML and variable references
databricks bundle validate

# Deploy to a specific environment
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod

# Run a specific job or pipeline
databricks bundle run orchestrator -t dev
databricks bundle run bronze_ingestion -t dev

# View deployed resources and URLs
databricks bundle summary -t dev

# Destroy deployed resources
databricks bundle destroy -t dev
```

### CI/CD Pipeline

**File:** `.github/workflows/deploy.yml`

**Triggers:** Push or PR to `main` branch (only changes in project directory)

```
PR to main:
  └── validate         → databricks bundle validate

Push to main:
  ├── validate         → databricks bundle validate
  ├── deploy-staging   → databricks bundle deploy --target staging
  └── deploy-prod      → databricks bundle deploy --target prod
                          (requires manual approval via GitHub environment)
```

**Required GitHub Secrets:**

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://adb-XXXXX.XX.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Service principal or PAT token |

---

## Adding a New Table

To ingest a new table, create a single YAML file — no code changes needed.

**Step 1:** Create `config/tables/adventureworks/sales_newtable.yml`:

```yaml
table:
  source: adventureworks
  source_schema: Sales
  source_table: NewTable
  target_name: newtable
  enabled: true
  execution_order: 25

  load:
    type: incremental
    watermark_column: ModifiedDate
    primary_key: [NewTableID]
    fetch_size: 10000

  schema_drift:
    mode: auto_evolve
    alert_on_drift: true
    allowed_changes: [add_column]

  quality:
    expectations:
      - name: "id_not_null"
        constraint: "NewTableID IS NOT NULL"
        action: fail

  silver:
    transformations:
      - type: trim_strings
        columns: all
    scd_type: 1
    merge_keys: [NewTableID]
```

**Step 2:** Deploy:

```bash
databricks bundle deploy -t dev
databricks bundle run orchestrator -t dev
```

The framework automatically picks up the new YAML and creates Bronze + Silver DLT tables.

**Step 3 (optional):** Add a Gold model if needed:

```yaml
# config/gold/dim_newtable.yml
gold_model:
  name: dim_newtable
  type: materialized_view
  source_tables: [silver.newtable]
  sql: |
    SELECT * FROM LIVE.newtable
  quality:
    expectations: []
```

---

## Adding a New Source System

**Step 1:** Create source config `config/sources/newsource.yml`:

```yaml
source:
  name: newsource
  type: jdbc
  connection:
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    host_secret_scope: "adls-secrets"
    host_secret_key: "newsource-host"
    port: 1433
    database: "NewDB"
    username_secret_key: "newsource-username"
    password_secret_key: "newsource-password"
    jdbc_options:
      encrypt: "true"
```

**Step 2:** Create table configs under `config/tables/newsource/`:

```yaml
# config/tables/newsource/orders.yml
table:
  source: newsource
  source_schema: dbo
  source_table: Orders
  target_name: newsource_orders
  enabled: true
  # ... rest of config
```

**Step 3:** Add secrets to Databricks:

```bash
databricks secrets put-secret adls-secrets newsource-host --string-value "server.database.windows.net"
databricks secrets put-secret adls-secrets newsource-username --string-value "user"
databricks secrets put-secret adls-secrets newsource-password --string-value "password"
```

**Step 4:** Deploy:

```bash
databricks bundle deploy -t dev
```

---

## Monitoring & Health Checks

**Monitoring Job** runs every 30 minutes and checks:

1. **Watermark Freshness** — Alerts if any watermark is older than 24 hours
2. **Table Row Counts** — Reports row counts per schema (Bronze/Silver/Gold)
3. **Pipeline Status** — Summary of environment health

**View in Databricks:**
- Workflows → `MDI_Monitoring_{env}` → Run History
- DLT Pipelines → Event Log → Data Quality metrics

---

## Deployed Resources

After `databricks bundle deploy`, these resources are created:

| Resource | Type | Name Pattern |
|----------|------|-------------|
| Bronze Pipeline | DLT Pipeline | `MDI_Bronze_Ingestion_{env}` |
| Silver Pipeline | DLT Pipeline | `MDI_Silver_Transform_{env}` |
| Gold Pipeline | DLT Pipeline | `MDI_Gold_Analytics_{env}` |
| Orchestrator | Workflow Job | `MDI_Orchestrator_{env}` |
| Monitoring | Workflow Job | `MDI_Monitoring_{env}` |

**Workspace Path:** `/Workspace/Users/{user}/.bundle/metadata-driven-ingestion/{env}/files`

---

## Secrets Configuration

Create the required secrets in Databricks:

```bash
# Create secret scope (if not exists)
databricks secrets create-scope adls-secrets

# SQL Server credentials
databricks secrets put-secret adls-secrets sqlserver-host \
  --string-value "your-server.database.windows.net"
databricks secrets put-secret adls-secrets sqlserver-username \
  --string-value "your-username"
databricks secrets put-secret adls-secrets sqlserver-password \
  --string-value "your-password"

# ADLS service principal (for Bronze external storage)
databricks secrets put-secret adls-secrets client-id \
  --string-value "your-sp-client-id"
databricks secrets put-secret adls-secrets client-secret \
  --string-value "your-sp-client-secret"
databricks secrets put-secret adls-secrets tenant-id \
  --string-value "your-tenant-id"
```

---

## Cluster & Performance

| Environment | Workers | Node Type | Autoscale | Photon |
|-------------|---------|-----------|-----------|--------|
| Dev | 1 | Standard_DS3_v2 (4 core, 14GB) | No | Yes |
| Staging | 2 | Standard_DS3_v2 | No | Yes |
| Prod | 4 | Standard_DS4_v2 (8 core, 28GB) | 2–8 | Yes |

**Spark Optimizations Enabled:**
- `spark.sql.adaptive.enabled = true` — Adaptive Query Execution
- `spark.databricks.delta.schema.autoMerge.enabled = true` — Schema evolution
- Photon engine enabled for Bronze and Silver layers

---

## License

Internal use — DAQ Consulting.
