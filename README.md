# Metadata-Driven Ingestion Framework

Production-style Databricks medallion framework powered by **Databricks Asset Bundles**, **Spark Declarative Pipelines**, and **metadata-driven YAML contracts**.

Primary source profile is currently **Unity Catalog `samples.tpch`**.

## Table of Contents

- [1. Executive Summary](#1-executive-summary)
- [2. Architecture](#2-architecture)
- [3. Runtime Topology](#3-runtime-topology)
- [4. Repository Structure](#4-repository-structure)
- [5. Naming Conventions](#5-naming-conventions)
- [6. Source and Table Scope](#6-source-and-table-scope)
- [7. Strategy Model (Bronze)](#7-strategy-model-bronze)
- [8. Silver Layer Behavior](#8-silver-layer-behavior)
- [9. Gold Layer Behavior](#9-gold-layer-behavior)
- [9.1 Table Type Decision Matrix (End-to-End)](#91-table-type-decision-matrix-end-to-end)
- [10. Configuration Contracts](#10-configuration-contracts)
- [11. Environments](#11-environments)
- [12. Deploy and Run](#12-deploy-and-run)
- [13. Monitoring and Health](#13-monitoring-and-health)
- [14. PLAN.md Alignment](#14-planmd-alignment)
- [15. Known Constraints](#15-known-constraints)

## 1. Executive Summary

This project implements a configurable ingestion framework with these core principles:

- **Metadata first**: source, table, quality, and model logic live in YAML.
- **Strategy-aware Bronze**: `snapshot`, `incremental`, and `cdc` strategy paths are supported.
- **Stateful ingestion**: control tables track ingestion and reconciliation metadata.
- **Contracted promotion path**: Bronze -> Silver -> Gold via orchestrated pipelines.
- **Environment portability**: dev/staging/prod behavior is parameterized through bundle targets.

Current active data domain is TPCH (`samples.tpch`) with curated dimension/fact outputs.

## 2. Architecture

```text
Unity Catalog Source
  samples.tpch.*
       |
       v
Bronze Pipeline (bronze_raw schema)
  raw_<table>    : audit/raw capture
  state_<table>  : current-state representation
       |
       v
Silver Pipeline (<silver_schema>)
  <table>             : cleansed/conformed
  quarantine_<table>  : warn-rule violations
       |
       v
Gold Pipeline (<gold_schema>)
  dimensions/facts + dim_date + pipeline_quality_kpis
       |
       v
Consumption (BI / Analytics)
```

## 3. Runtime Topology

### Databricks resources

- Pipeline: `MDI_Bronze_Ingestion_<env>`
- Pipeline: `MDI_Silver_Transform_<env>`
- Pipeline: `MDI_Gold_Analytics_<env>`
- Job: `MDI_Orchestrator_<env>`
- Job: `MDI_Monitoring_<env>`

### Orchestration order

1. `bronze_ingestion`
2. `silver_transform`
3. `gold_analytics`

## 4. Repository Structure

```text
.
├── databricks.yml
├── PLAN.md
├── config/
│   ├── environments/
│   │   ├── dev.yml
│   │   ├── staging.yml
│   │   └── prod.yml
│   ├── sources/
│   │   └── tpch_samples.yml
│   ├── tables/
│   │   └── tpch/samples/tpch/
│   │       ├── region.yml
│   │       ├── nation.yml
│   │       ├── customer.yml
│   │       ├── supplier.yml
│   │       ├── part.yml
│   │       ├── partsupp.yml
│   │       ├── orders.yml
│   │       └── lineitem.yml
│   └── gold/
│       ├── dim_region_nation.yml
│       ├── dim_customer.yml
│       ├── dim_supplier.yml
│       ├── dim_part.yml
│       ├── fact_order_line.yml
│       └── pipeline_quality_kpis.yml
├── resources/
│   ├── bronze_dlt_pipeline.yml
│   ├── silver_dlt_pipeline.yml
│   ├── gold_dlt_pipeline.yml
│   ├── orchestrator_job.yml
│   └── monitoring_job.yml
├── src/
│   ├── bronze/bronze_dlt.py
│   ├── silver/silver_dlt.py
│   ├── gold/gold_dlt.py
│   ├── framework/
│   │   ├── config_reader.py
│   │   ├── source_connectors.py
│   │   ├── quality_engine.py
│   │   ├── schema_drift.py
│   │   ├── watermark_manager.py
│   │   └── state_manager.py
│   └── utils/health_check.py
└── .github/workflows/deploy.yml
```

## 5. Naming Conventions

### Bronze

- Raw datasets: `raw_<target_name>`
- State datasets: `state_<target_name>`
- Schema: `${var.bronze_raw_schema}` (default `bronze_raw`)

### Silver

- Clean datasets: `<target_name>`
- Quarantine datasets: `quarantine_<target_name>`
- Schema: `${var.silver_schema}`

### Gold

- Dim/fact models from `config/gold/*.yml`
- Auto models:
  - `dim_date`
  - `pipeline_quality_kpis`
- Schema: `${var.gold_schema}`

### Control

Control tables are created under `${var.control_schema}`:

- `watermarks`
- `batch_state`
- `reconciliation_log`
- `source_metadata`

## 6. Source and Table Scope

### Active source

File: `config/sources/tpch_samples.yml`

- `type: unity_catalog`
- `connection.catalog: samples`

### Active TPCH tables

- `region` (order 5)
- `nation` (order 10)
- `customer` (order 20)
- `supplier` (order 25)
- `part` (order 30)
- `partsupp` (order 35)
- `orders` (order 40)
- `lineitem` (order 50)

### Current strategy status (important)

For the current TPCH setup:

- All Bronze table configs are `load.strategy: snapshot`
- There are currently no `incremental` tables
- There are currently no `cdc`/true stream tables
- `snapshot_audit.enabled` is `false` for all TPCH tables, so `raw_<table>` audit datasets are not materialized in normal runs

## 7. Strategy Model (Bronze)

Implemented in `src/bronze/bronze_dlt.py`.

### `snapshot`

- Reads full source snapshot.
- Builds temporary snapshot source view.
- Optional raw snapshot audit table (`snapshot_audit.enabled`).
- Applies to `state_<table>` via `create_auto_cdc_from_snapshot_flow` when available.
- Falls back to materialized view state if API support is unavailable.
- In this path, `state_<table>` can be a **streaming table** even when source is not a real-time stream.

### `incremental`

- Reads incremental deltas (watermark-driven where configured by source connector).
- Persists into `raw_<table>`.
- Produces `state_<table>` by latest-record-per-key fallback logic.

### `cdc`

- Reads CDC stream into `raw_<table>`.
- Applies into `state_<table>` via `create_auto_cdc_flow` when API support exists.
- Falls back to streaming passthrough state table otherwise.
- This is the path that represents true stream/CDC ingestion semantics.

## 8. Silver Layer Behavior

Implemented in `src/silver/silver_dlt.py`.

- Reads Bronze **state** datasets.
- Applies table-level transformations from YAML.
- Applies expectation actions (`warn`, `drop`, `fail`).
- Writes conformed Silver datasets.
- Emits quarantine datasets for `warn` rules.
- Supports SCD2 flow path when `scd_type: 2` and APIs are available.

Decision rules in code:

- `streaming_mode = (silver.mode == "streaming") OR (load.strategy in {"cdc","stream"}) OR (load_type == "stream")`
- If `scd_type == 2` and not `streaming_mode`:
  - Uses snapshot SCD2 flow (`create_auto_cdc_from_snapshot_flow`) targeting a streaming table
- Else:
  - `streaming_mode = true` -> `@dp.table` (streaming table path)
  - `streaming_mode = false` -> `@dp.materialized_view` (batch MV path)

Current TPCH behavior:

- All Silver datasets run in batch mode (`silver.mode: batch`)
- Silver output tables are materialized view path (plus quarantine MVs when warn rules exist)

## 9. Gold Layer Behavior

Implemented in `src/gold/gold_dlt.py`.

- Loads model configs from `config/gold/*.yml`.
- Resolves SQL references (`silver.<table>`) to fully-qualified catalog/schema names.
- Orders model execution using `depends_on`.
- Supports:
  - SCD1 materialized views
  - SCD2 snapshot flow models (`scd_type: 2` + `natural_key`)
- Auto-builds:
  - `dim_date`
  - `pipeline_quality_kpis`

Current model outcomes:

- `dim_customer` has `scd_type: 2` -> created via SCD2 snapshot flow path (streaming-table target)
- Other gold models (`dim_region_nation`, `dim_supplier`, `dim_part`, `fact_order_line`) are MVs
- Auto models `dim_date` and `pipeline_quality_kpis` are MVs

### 9.1 Table Type Decision Matrix (End-to-End)

This project decides table type from metadata, not from layer name alone.

`Bronze`:

- `load.strategy = snapshot`
  - `state_<table>`: streaming table when snapshot AUTO CDC API is available
  - Fallback: materialized view state
  - `raw_<table>` only if `snapshot_audit.enabled: true`
- `load.strategy = incremental`
  - `raw_<table>`: materialized view
  - `state_<table>`: materialized view (latest-per-key approximation)
- `load.strategy in {cdc, stream}`
  - `raw_<table>`: streaming table (`@dp.table`)
  - `state_<table>`: streaming table with `create_auto_cdc_flow` (or streaming fallback)

`Silver`:

- `streaming_mode = true` -> standard table path (`@dp.table`)
- `streaming_mode = false` -> materialized view path
- `scd_type = 2` with non-stream mode -> snapshot SCD2 flow path (streaming table target), otherwise fallback MV

`Gold`:

- `scd_type = 1` -> materialized view
- `scd_type = 2` + `natural_key` + API support -> streaming table + `create_auto_cdc_from_snapshot_flow`
- If SCD2 prerequisites are missing -> fallback MV

Important clarification:

- `streaming table` in DLT/Lakeflow is an execution/state-management construct.
- It does **not** always mean the upstream source is Kafka/Event Hub real-time stream.
- In this project, snapshot-driven SCD2/state handling can still produce streaming tables.

Managed/external vs MV/streaming are different axes:

- Storage axis: managed vs external (where data files are stored/managed)
- Processing axis: materialized view vs streaming table (how updates are processed)
- A table can be managed+streaming, managed+MV, or external+MV depending on configuration

## 10. Configuration Contracts

### Source contract

`config/sources/<source>.yml`

Core fields:

- `source.name`
- `source.type`
- `source.connection.*`

### Table contract

`config/tables/<source>/<db>/<schema>/<table>.yml`

Core fields:

- `table.source`, `table.source_schema`, `table.source_table`, `table.target_name`
- `table.execution_order`
- `table.load.strategy` (`snapshot|incremental|cdc`)
- `table.load.primary_key`
- `table.load.stored_as_scd_type`
- `table.load.sequence_by_column`
- `table.load.snapshot_audit`
- `table.load.reconciliation`
- `table.load.cdc`
- `table.schema_drift.*`
- `table.quality.expectations[]`
- `table.silver.mode`
- `table.silver.transformations[]`
- `table.silver.scd_type`
- `table.silver.merge_keys`

### Gold model contract

`config/gold/<model>.yml`

Core fields:

- `gold_model.name`
- `gold_model.type` (`dimension|fact|monitoring`)
- `gold_model.scd_type`
- `gold_model.natural_key` (required for SCD2)
- `gold_model.depends_on`
- `gold_model.source_tables`
- `gold_model.sql`
- `gold_model.quality.expectations[]`

## 11. Environments

Environment overlays live in `config/environments/*.yml` and bundle target variables live in `databricks.yml`.

### `dev`

- `catalog: dev_catalog`
- `silver_schema: dev_silver`
- `gold_schema: dev_gold`
- `num_workers: 1`

### `staging`

- `catalog: staging_catalog`
- `silver_schema: staging_silver`
- `gold_schema: staging_gold`
- `num_workers: 2`

### `prod`

- `catalog: prod_catalog`
- `silver_schema: silver_data`
- `gold_schema: gold_analytics`
- `num_workers: 4`

## 12. Deploy and Run

```bash
# Validate bundle (dev)
databricks bundle validate -t dev --profile DEFAULT

# Deploy bundle (dev)
databricks bundle deploy -t dev --profile DEFAULT

# Show deployed resources
databricks bundle summary -t dev --profile DEFAULT

# Trigger orchestrator
databricks bundle run orchestrator -t dev --profile DEFAULT
```

If deploy prompts for recreation confirmation:

```bash
databricks bundle deploy -t dev --profile DEFAULT --auto-approve
```

## 13. Monitoring and Health

Monitoring job executes `src/utils/health_check.py` and checks:

- watermark freshness from `<catalog>.<control_schema>.watermarks`
- row-count summaries for Bronze/Silver/Gold schemas
- environment-scoped health output

Monitoring schedule:

- cron: `0 */30 * * * ?`
- timezone: `America/New_York`

## 14. PLAN.md Alignment

Major plan-level gaps that are now implemented in codebase:

- strategy-aware Bronze model
- control schema state tables
- Silver quarantine path
- Gold dependency ordering
- Gold SCD2-capable model path
- monitoring KPI model

Current deliberate deviations:

- physical `bronze_state` **separate schema** is not used; state datasets are `state_*` inside Bronze raw schema.
- 2-pipeline target topology from plan is not applied; runtime keeps 3 dedicated layer pipelines.

## 15. Known Constraints

1. `bundle run` and direct API commands can intermittently fail if workspace host DNS resolution is unstable.
2. CI workflow currently uses `metadata-driven-ingestion` as working directory/path filter; adjust if repo root remains `daq-databricks-dab`.
3. Local `.DS_Store` files are non-runtime artifacts and should be ignored in version control.
