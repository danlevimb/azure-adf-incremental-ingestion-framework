# Architecture Overview

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This project implements a metadata-driven incremental ingestion framework using Azure Data Factory.

The framework ingests data from:

- Local SQL Server tables
- CSV files
- JSON files

and lands the data into Azure Data Lake Storage Gen2 using parameterized pipelines, control metadata, watermarks, operational logging, and evidence-based validation.

The goal is to demonstrate a professional Azure Data Engineering ingestion pattern rather than a simple one-off copy pipeline.

## High-Level Architecture

![High level architecture](../../diagrams/01_high_level_architecture.png)


## Main Components

| **Component** | **Role** |
|-----------|------|
| SQL Server Local | Primary source system and control metadata store for the MVP. |
| Self-hosted Integration Runtime | Allows Azure Data Factory to connect to the local SQL Server environment. |
| Azure Data Factory | Orchestrates metadata lookup, incremental extraction, file ingestion, failure handling, and child pipeline execution. |
| Azure Data Lake Storage Gen2 | Stores landed SQL, CSV, and JSON outputs. |
|Control Metadata Tables | Store source configuration, run history, watermark state, and failure/retry evidence. |
| Sample CSV / JSON Files |	Secondary file-based sources used to validate multi-source ingestion. |

## Source Systems

### SQL Server Local

The main source system is a local SQL Server database:

```text
ADF_Ingestion_Source
```

Main source tables:

```sql 
dbo.Customers
dbo.Products
dbo.Orders
dbo.OrderItems
dbo.Payments
```

Each table includes:

```text 
CreatedAt
UpdatedAt
```

The framework uses UpdatedAt as the datetime watermark column.

### File-Based Sources

The framework also supports file ingestion from ADLS Gen2 landing.

Supported formats:

```txt
CSV
JSON
```

Sample file objects:

```text 
currency_rates.csv
country_currency.csv
source_system_metadata.json
manual_adjustments.json
```

### Target Storage

The target platform is Azure Data Lake Storage Gen2.

Approved containers:

```text
landing
bronze
rejected
metadata
evidence
```

### SQL Output Pattern

SQL Server extraction outputs are written as Parquet files under bronze:

```text 
bronze/sqlserver/<source_system>/<schema>/<table>/load_date=YYYY-MM-DD/run_id=<run_id>/<table>.parquet
```

Example:

```text 
bronze/sqlserver/sales_local/dbo/orders/load_date=2026-05-15/run_id=<guid>/orders.parquet
```

### File Output Pattern

CSV and JSON file outputs are copied from landing into bronze:

```text 
bronze/files/<format>/<file_object>/load_date=YYYY-MM-DD/run_id=<run_id>/<file_name>
```

Example:

```text 
bronze/files/json/source_system_metadata/load_date=2026-05-15/run_id=<guid>/source_system_metadata.json
```
### Control Metadata Layer

The framework uses a SQL Server ctl schema for metadata-driven orchestration.

Core control tables:

| Table | Purpose |
|-------|---------|
| `ctl.SourceObject` | Defines source objects, source type, destination paths, load type, and watermark state. |
| `ctl.FileSourceConfig` | Stores file-specific configuration such as file format, source path, file name pattern, delimiter, and header behavior. |
| `ctl.IngestionRun` | Records ingestion run status, row counts, watermarks, destination folders, errors, start/end times, and duration. |
| `ctl.IngestionRunStep` | Stores optional step-level execution details. |
| `ctl.WatermarkHistory` | Tracks watermark movement after successful ingestion runs. |

### Watermark Strategy

The SQL incremental ingestion uses a datetime-based high watermark strategy.

Approved watermark column:

```text 
UpdatedAt
```

Initial low watermark:

```text
1900-01-01 00:00:00.000
```

Extraction window:

```sql 
WHERE UpdatedAt > @LastWatermarkValue
  AND UpdatedAt <= @CurrentHighWatermarkValue
```

Core rules:

1. Read the previous watermark from ctl.SourceObject.
2. Capture the current high watermark before copying data.
3. Copy only rows inside the extraction window.
4. Complete the ingestion run only after successful copy.
5. Update the stored watermark only after successful ingestion.
6. Insert watermark history only for successful runs.
7. Do not advance the watermark when a run fails.

### Azure Data Factory Pipelines

The ADF implementation uses a master/child pipeline design.

| Pipeline | Purpose |
|----------|---------|
| `PL_00_Master_Ingestion_Orchestrator` | Reads active source objects and routes execution to SQL or file ingestion pipelines. |
| `PL_01_SQL_Incremental_Ingestion` | Performs metadata-driven SQL incremental extraction into ADLS Gen2 Parquet.|
| `PL_02_File_Ingestion` | Copies CSV and JSON files from landing into bronze using dynamic datasets. |

### Linked Services

| Linked Service | Purpose |
|----------------|---------|
| `LS_SQLSERVER_LOCAL_SHIR` | Connects ADF to local SQL Server source tables through Self-hosted Integration Runtime. |
| `LS_CONTROL_SQLSERVER_LOCAL` | Connects ADF to local SQL Server control metadata through Self-hosted Integration Runtime. |
| `LS_ADLSGEN2_DEV` | Connects ADF to Azure Data Lake Storage Gen2 using managed identity. |

### Dynamic Datasets

| Dataset | Purpose |
|---------|---------|
| `DS_SQLSERVER_TABLE_DYNAMIC` | Reads SQL Server tables dynamically using schema and table parameters. |
| `DS_ADLS_PARQUET_DYNAMIC` | Writes SQL extraction outputs to dynamic ADLS Gen2 Parquet paths. |
| `DS_ADLS_CSV_DYNAMIC` | Reads and writes CSV files using dynamic ADLS paths.
| `DS_ADLS_JSON_DYNAMIC` | Reads and writes JSON files using | dynamic ADLS paths.

### Operational Validation

The framework was validated with the following scenarios:

| Scenario | Result |
|----------|--------|
| Initial SQL ingestion | Successful |
| Incremental insert | Successful |
 Incremental update | Successful |
| Refund scenario | Successful |
| Empty run validation | Successful |
| Controlled failed run | Successful |
| Retry after failure | Successful |
| CSV file ingestion | Successful |
| JSON file ingestion | Successful |
| Master orchestration for SQL sources | Successful |
| Master orchestration for file sources | Successful |

### Failure Handling

The SQL ingestion pipeline includes a failure path.

If the copy activity fails:

1. The run is marked as Failed in ctl.IngestionRun.
2. NewWatermarkValue remains NULL.
3. No record is inserted into ctl.WatermarkHistory.
4. The source row remains eligible for retry.
5. A later successful retry can process the pending row and advance the watermark.

This behavior was validated through a controlled failure and retry scenario.

### Architecture Value

This architecture demonstrates:

* Metadata-driven ingestion
* Hybrid connectivity through Self-hosted Integration Runtime
* Dynamic ADF pipelines and datasets
* SQL Server to ADLS Gen2 incremental loading
* CSV and JSON file ingestion
* Watermark governance
* Operational logging
* Failure-safe ingestion behavior
* Retry readiness
* Evidence-driven validation

The project is intentionally scoped as an MVP, but it follows patterns commonly found in real-world data engineering workflows.