# Metadata-Driven Ingestion Framework — Implementation Plan v3

> **Date**: 2026-02-24
> **Status**: Approved for implementation
> **Runtime**: Databricks DBR 18.0+ (Spark 4.1), Lakeflow SDP
> **Bundle**: Databricks Asset Bundles (DAB)

---

## 1. Architecture Overview

### Medallion Architecture with SDP

```
Source Systems                    Databricks (Unity Catalog)
─────────────────                 ──────────────────────────
SQL Server (JDBC)  ──┐
Oracle (JDBC)      ──┤           ┌─── bronze_raw ──────────────────┐
SFTP (files)       ──┼──────────▶│  Append-only streaming tables   │
Kafka (CDC)        ──┤           │  Audit trail, immutable         │
Auto Loader        ──┘           └────────────┬────────────────────┘
                                              │
                                 ┌─── bronze_state ────────────────┐
                                 │  Current-state streaming tables │
                                 │  Flow-owned (AUTO CDC)          │
                                 └────────────┬────────────────────┘
                                              │
                                 ┌─── silver ──────────────────────┐
                                 │  Cleansed, conformed            │
                                 │  Streaming tables, SCD Type 1   │
                                 │  Quality expectations           │
                                 └────────────┬────────────────────┘
                                              │
                                 ┌─── gold ────────────────────────┐
                                 │  Star schema, BI-ready          │
                                 │  SCD2 dims: Streaming Table     │
                                 │  SCD1 dims: Materialized View   │
                                 │  Facts:     Materialized View   │
                                 └────────────┬────────────────────┘
                                              │
                                         Power BI
```

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Bronze raw vs state | Separate schemas | Raw = audit/replay, state = current for downstream |
| Bronze table type | Streaming Table (flow-owned) | `create_streaming_table()` + AUTO CDC flows |
| Silver table type | Streaming Table | Clean current state, SCD Type 1 |
| Gold SCD2 dims | Streaming Table + AUTO CDC | History tracking requires CDC flow, not MV |
| Gold SCD1 dims | Materialized View | Simple, no history needed |
| Gold facts | Materialized View | Pre-computed aggregations, JOINs |
| Pipelines | 2 (ingest+conform, serve) | Different cadence/compute profiles |
| Config | YAML per table | Git-versioned, PR-reviewable, rollback-friendly |
| State management | control schema | Watermarks, batch state, reconciliation |

---

## 2. Schema Layout

```
{catalog}/                              # dev_catalog | staging_catalog | prod_catalog
├── bronze_raw/                         # Append-only audit trail
│   └── raw_{table_name}                # Streaming table, immutable
├── bronze_state/                       # Current state
│   └── {table_name}                    # Streaming table, flow-managed
├── silver/                             # Cleansed, conformed
│   └── {table_name}                    # Streaming table, SCD1
├── silver_quarantine/                  # Quality failures (action: warn)
│   └── {table_name}_quarantine         # Append-only
├── gold/                               # Star schema
│   ├── dim_{name}                      # ST+CDC (SCD2) or MV (SCD1)
│   ├── fact_{name}                     # MV (aggregations)
│   ├── dim_date                        # MV (auto-generated)
│   └── pipeline_quality_kpis           # MV (monitoring)
└── control/                            # Framework state
    ├── watermarks                      # Per-table watermark values
    ├── batch_state                     # Monotonic batch_id per table
    ├── reconciliation_log              # Weekly reconciliation results
    └── source_metadata                 # Schema versions, drift log
```

---

## 3. Load Strategies

### Strategy Selection Matrix

```
New table → What size? → CDC/CT available? → Strategy:

< 1M rows (any)           → strategy: snapshot
1M-100M rows, no CDC      → strategy: incremental + reconciliation
1M-100M rows, CDC/CT      → strategy: cdc
100M+ rows, no CDC        → ❌ Enable CDC on source (mandatory)
100M+ rows, CDC/CT        → strategy: cdc
```

### Strategy Details

#### `snapshot` (small/medium tables, < 1M rows)
- **Extract**: Full `SELECT *` from source every run (full scan)
- **Apply**: Engine diffs snapshots → incremental INSERT/UPDATE/DELETE
- **Delete detection**: Yes (missing rows = deleted)
- **SDP API**: `create_streaming_table()` + `create_auto_cdc_from_snapshot_flow()`
- **Version management**: Monotonic version from `control.batch_state`
- **Bronze raw**: Optional (snapshot_audit.enabled in YAML)

#### `incremental` (medium tables, no CDC available)
- **Extract**: Watermark-based `WHERE ModifiedDate > last_watermark`
- **Apply**: Append to `bronze_raw`, merge/upsert into `bronze_state`
- **Delete detection**: No (requires periodic reconciliation)
- **Reconciliation**: Weekly full snapshot compare (configurable)
- **Important**: NEVER append directly to `bronze_state` — always merge

#### `cdc` (large tables, CDC/CT enabled on source)
- **Extract**: Change log only (INSERT/UPDATE/DELETE events)
- **Apply**: `create_auto_cdc_flow()` into `bronze_state`
- **Delete detection**: Yes (DELETE events in change log)
- **Source types (v1)**: `kafka_debezium`, `lakeflow_connect`
- **Source types (roadmap)**: `custom_sqlserver_ct_reader`

### Incremental Apply vs Incremental Extract

> **Critical distinction**: Snapshot strategy has **incremental apply** (Databricks side)
> but **full extract** (source side). For truly incremental extract, use CDC/CT.

---

## 4. Table Type Summary

| Schema | Table Type | How Created | Managed By |
|--------|-----------|-------------|------------|
| bronze_raw | Streaming Table | `create_streaming_table()` | Append flow |
| bronze_state | Streaming Table | `create_streaming_table()` | AUTO CDC flow |
| silver | Streaming Table | `create_streaming_table()` | Transform flow |
| silver_quarantine | Streaming Table | `create_streaming_table()` | Quality engine |
| gold dim (SCD2) | Streaming Table | `create_streaming_table()` | AUTO CDC flow (scd_type=2) |
| gold dim (SCD1) | Materialized View | `@dp.materialized_view()` | Decorator (dataset fn) |
| gold fact | Materialized View | `@dp.materialized_view()` | Decorator (dataset fn) |
| gold dim_date | Materialized View | `@dp.materialized_view()` | Decorator (dataset fn) |
| gold monitoring | Materialized View | `@dp.materialized_view()` | Decorator (dataset fn) |
| control.* | Regular Delta | `CREATE TABLE` (outside SDP) | State manager |

---

## 5. SCD2 Column Standard

All SCD Type 2 dimension tables use these standardized columns:

| Column | Type | Description |
|--------|------|-------------|
| `valid_from` | TIMESTAMP | Row effective start time |
| `valid_to` | TIMESTAMP | Row effective end time (9999-12-31 for current) |
| `is_current` | BOOLEAN | TRUE for current version |

- Generated automatically by `AUTO CDC flow (stored_as_scd_type=2)`
- `sequence_by_column` must be deterministic (source column like `ModifiedDate`, NOT Spark timestamp)
- Fact tables join on `dim.is_current = TRUE` for current-state queries

---

## 6. Quality Strategy

### Expectation Actions

| Action | Behavior | Quarantine |
|--------|----------|------------|
| `fail` | Pipeline fails if any row violates | No |
| `drop` | Bad rows silently dropped | No |
| `warn` | Bad rows kept + logged | Yes → `silver_quarantine.{table}_quarantine` |

### Quarantine Table
- Append-only: bad rows + violation reason + timestamp
- Queryable for audit and data quality reporting

### Monitoring
- `gold.pipeline_quality_kpis` MV reads from SDP event log
- Tracks: row counts, expectation pass/fail rates, processing times
- Queryable from Power BI for operational dashboards

---

## 7. Project Structure

```
metadata-driven-ingestion/
├── databricks.yml                          # Bundle config, targets, variables
├── PLAN.md                                 # This document
├── resources/
│   ├── ingest_conform_pipeline.yml         # Pipeline 1: Bronze + Silver
│   ├── serve_pipeline.yml                  # Pipeline 2: Gold
│   └── orchestrator_job.yml                # Workflow: P1 → P2
├── config/
│   ├── sources/
│   │   └── adventureworks.yml              # JDBC connection (secret refs)
│   ├── tables/
│   │   └── {source}/{database}/{schema}/
│   │       └── {Table}.yml                 # Per-table config
│   ├── gold/
│   │   ├── dim_customer.yml                # SCD2 dimension
│   │   ├── dim_territory.yml               # SCD1 dimension
│   │   ├── fact_sales.yml                  # Fact table
│   │   └── ...
│   └── environments/
│       ├── dev.yml
│       ├── staging.yml
│       └── prod.yml
├── src/
│   ├── framework/
│   │   ├── config_reader.py                # YAML load, parse, env merge
│   │   ├── source_connectors.py            # Factory: JDBC, Kafka, Auto Loader
│   │   ├── quality_engine.py               # Expectations + quarantine logic
│   │   └── state_manager.py                # Watermarks, batch_state, reconciliation
│   ├── bronze/
│   │   └── bronze_sdp.py                   # Strategy-based Bronze ingestion
│   ├── silver/
│   │   └── silver_sdp.py                   # Transformations + quality
│   └── gold/
│       └── gold_sdp.py                     # SCD2 dims (ST+CDC), facts (MV)
├── tests/
│   └── ...
└── .github/
    └── workflows/
        └── deploy.yml                      # CI/CD: validate → deploy
```

---

## 8. YAML Config Specification

### Table Config (`config/tables/{source}/{db}/{schema}/{Table}.yml`)

```yaml
table:
  source: adventureworks                    # References config/sources/{name}.yml
  database: AdventureWorks2022
  source_schema: Sales
  source_table: SalesOrderHeader
  target_name: salesorderheader             # Target table name in Databricks
  enabled: true
  execution_order: 20                       # Parallel execution grouping

  load:
    strategy: snapshot                      # snapshot | incremental | cdc
    primary_key: [SalesOrderID]
    stored_as_scd_type: 1                   # 1 = current state in bronze_state
    sequence_by_column: ModifiedDate        # Deterministic! Source column.

    # Snapshot strategy options
    snapshot_audit:
      enabled: false                        # Opt-in: append snapshots to bronze_raw
      retention_days: 90
      partition_by: _batch_id

    # Incremental strategy options (when strategy=incremental)
    # watermark_column: ModifiedDate
    # fetch_size: 10000
    # reconciliation:
    #   enabled: true
    #   schedule: weekly
    #   method: full_snapshot               # full_snapshot | rowcount | key_hash

    # CDC strategy options (when strategy=cdc)
    # cdc:
    #   source_type: kafka_debezium         # kafka_debezium | lakeflow_connect
    #   topic: dbserver1.Sales.SalesOrderHeader

  schema_drift:
    mode: auto_evolve                       # auto_evolve | strict
    alert_on_drift: true

  quality:
    expectations:
      - name: order_id_not_null
        constraint: "SalesOrderID IS NOT NULL"
        action: fail
      - name: valid_total_due
        constraint: "TotalDue >= 0"
        action: warn                        # warn → kept + quarantined

  silver:
    mode: batch                             # streaming | batch (per table)
    transformations:
      - type: trim_strings
        columns: all
      - type: null_standardize
        values: ["NULL", "N/A", ""]
      - type: add_column
        name: order_year
        expression: "YEAR(OrderDate)"
      - type: cast
        column: TotalDue
        target_type: decimal(19,4)
      - type: rename_column
        from: ModifiedDate
        to: source_modified_date
      - type: drop_columns
        columns: [rowguid]
```

### Gold Model Config (`config/gold/{model_name}.yml`)

```yaml
model:
  name: dim_customer
  description: "Customer dimension with SCD Type 2 history"
  type: dimension                           # dimension | fact
  scd_type: 2                               # 2 → ST + AUTO CDC, 1 → MV
  natural_key: [CustomerID]
  sequence_by_column: ModifiedDate          # Required for SCD2
  track_history_columns: [store_name, TerritoryID]
  depends_on: []                            # Explicit ordering
  source_tables:
    - silver.customer
    - silver.store
  sql: |
    SELECT
      c.CustomerID,
      c.PersonID,
      s.Name AS store_name,
      c.TerritoryID
    FROM customer c
    LEFT JOIN store s ON c.StoreID = s.BusinessEntityID
  quality:
    expectations:
      - name: customer_id_not_null
        constraint: "CustomerID IS NOT NULL"
        action: fail
```

---

## 9. Critical Implementation Notes

### 9.1 Python Closure Trap (Gold MVs in loops)

**Problem**: Creating `@dp.materialized_view` in a loop causes all MVs to reference the last model.

**Solution**: Factory function pattern:

```python
def make_gold_mv(name, sql, expectations):
    @dp.materialized_view(
        name=name,
        table_properties={"quality": "gold"},
    )
    @dp.expect_all(expectations["warn"])
    @dp.expect_all_or_drop(expectations["drop"])
    @dp.expect_all_or_fail(expectations["fail"])
    def _mv():
        return spark.sql(sql)
    return _mv
```

### 9.2 AUTO CDC Flow Source Must Be Table/View Name

**Problem**: `create_auto_cdc_flow(source=sql_string)` won't work.

**Solution**: Create intermediate temporary view first, then reference by name:

```python
# Create enriched view
@dp.temporary_view(name="silver_customer_enriched")
def _enriched():
    return spark.sql("SELECT c.*, s.Name ... FROM customer c LEFT JOIN store s ...")

# Then reference by name
dp.create_auto_cdc_flow(
    target="dim_customer",
    source="silver_customer_enriched",   # table/view name, not SQL
    keys=["CustomerID"],
    sequence_by=col("ModifiedDate"),
    stored_as_scd_type=2
)
```

### 9.3 Snapshot Version Must Be Deterministic

**Problem**: Retry/backfill can cause version conflicts.

**Solution**: Use `control.batch_state` for monotonic version generation:

```python
def get_next_version(table_name):
    """Get next monotonic version from control.batch_state."""
    result = spark.sql(f"SELECT MAX(batch_id) FROM control.batch_state WHERE table_name = '{table_name}'")
    current = result.collect()[0][0] or 0
    return current + 1
```

### 9.4 Incremental Strategy: Append Raw, Merge State

**Rule**: NEVER append directly to `bronze_state`. Always:
1. Append to `bronze_raw` (watermark-filtered rows)
2. Merge/upsert into `bronze_state` (idempotent, key-based)

### 9.5 Gold Pipeline Ordering

**Rule**: In serve pipeline, process in order:
1. SCD2 dimensions first (streaming tables + AUTO CDC flows)
2. SCD1 dimensions (materialized views)
3. Fact tables (materialized views, depend on dims)
4. Monitoring MV (last)

Use `depends_on` in YAML to enforce ordering.

---

## 10. Implementation Steps

### Step 1: Create Catalogs
```sql
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE CATALOG IF NOT EXISTS staging_catalog;
CREATE CATALOG IF NOT EXISTS prod_catalog;
```

### Step 2: Update databricks.yml
- New variables: `bronze_raw_schema`, `bronze_state_schema`, `silver_quarantine_schema`, `control_schema`
- Update targets with new catalog names

### Step 3: Refactor src/framework/
- `config_reader.py` — New fields: strategy, snapshot_audit, silver.mode, sequence_by_column, reconciliation, cdc
- `source_connectors.py` — Clean separation: batch read (JDBC) vs stream read (Kafka)
- `quality_engine.py` — Add quarantine table creation and routing
- `state_manager.py` — NEW: watermarks, batch_state, reconciliation_log (replaces watermark_manager.py)

### Step 4: Rewrite src/bronze/bronze_sdp.py
- Strategy dispatcher: snapshot / incremental / cdc
- `create_streaming_table()` + appropriate flow per strategy
- Bronze raw (opt-in) + bronze state (always)

### Step 5: Rewrite src/silver/silver_sdp.py
- Read from bronze_state (streaming or batch per table config)
- Apply YAML transformations
- Quality expectations + quarantine routing
- Write to silver streaming table

### Step 6: Rewrite src/gold/gold_sdp.py
- SCD2 dims: `create_streaming_table()` + `create_auto_cdc_flow(stored_as_scd_type=2)`
- Source for SCD2: intermediate `@dp.temporary_view` from Silver
- SCD1 dims: `@dp.materialized_view()` via factory function
- Facts: `@dp.materialized_view()` via factory function
- dim_date: auto-generated MV
- pipeline_quality_kpis: monitoring MV

### Step 7: Update resources/
- `ingest_conform_pipeline.yml` — Pipeline 1 (Bronze + Silver)
- `serve_pipeline.yml` — Pipeline 2 (Gold)
- `orchestrator_job.yml` — Job: P1 → P2 with schedule

### Step 8: Update YAML configs
- Table configs: add strategy, sequence_by_column, snapshot_audit, silver.mode
- Gold configs: add type, scd_type, natural_key, sequence_by_column, depends_on
- Environment configs: update catalog names, add new schema variables

### Step 9: Deploy & Test
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run orchestrator -t dev
```

### Step 10: Commit & Push
- Commit all changes
- Push to remote
- Verify CI/CD pipeline

---

## 11. Future Roadmap

| Feature | Priority | Description |
|---------|----------|-------------|
| Lakeflow Connect | High | Managed SQL Server connector (when network allows) |
| Custom CT reader | Medium | Direct SQL Server Change Tracking reader |
| Schema drift alerts | Medium | Slack/Teams notifications on schema changes |
| Data lineage dashboard | Low | Power BI dashboard from SDP event log |
| Multi-source JOINs in Gold | Low | Cross-source dimension enrichment |
| Auto-scaling clusters | Low | Per-pipeline compute optimization |
