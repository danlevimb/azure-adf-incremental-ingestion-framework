# Pipeline Build

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the Azure Data Factory pipeline build for the ADF Incremental Ingestion Framework.

The pipeline implementation converts the architecture into working ADF assets that support:

- Metadata-driven SQL Server ingestion
- CSV and JSON file ingestion
- Master/child orchestration
- Dynamic dataset parameterization
- Control table interaction
- Watermark updates
- Failure logging
- Retry-safe behavior
- Operational validation evidence

---

## ADF Asset Summary

The implementation includes:

| Asset Type | Assets |
|---|---|
| Linked Services | SQL Server source, SQL Server control metadata, ADLS Gen2 |
| Integration Runtimes | Self-hosted IR and AutoResolveIntegrationRuntime |
| Datasets | SQL Server, ADLS Parquet, ADLS CSV, ADLS JSON |
| Pipelines | Master orchestrator, SQL incremental ingestion, file ingestion |
| Control Interface | Stored procedures in the SQL Server `ctl` schema |

---

## Linked Services

The project uses three linked services:

| Linked Service | Purpose |
|---|---|
| `LS_SQLSERVER_LOCAL_SHIR` | Connects ADF to SQL Server source tables through Self-hosted Integration Runtime. |
| `LS_CONTROL_SQLSERVER_LOCAL` | Connects ADF to SQL Server control metadata through Self-hosted Integration Runtime. |
| `LS_ADLSGEN2_DEV` | Connects ADF to ADLS Gen2 using managed identity. |

---

## Datasets

The project uses four dynamic datasets:

| Dataset | Purpose |
|---|---|
| `DS_SQLSERVER_TABLE_DYNAMIC` | Dynamic SQL Server table access using schema and table parameters. |
| `DS_ADLS_PARQUET_DYNAMIC` | Dynamic Parquet output paths for SQL extraction results. |
| `DS_ADLS_CSV_DYNAMIC` | Dynamic CSV source and sink paths. |
| `DS_ADLS_JSON_DYNAMIC` | Dynamic JSON source and sink paths. |

---

## Pipeline Inventory

| Pipeline | Purpose |
|---|---|
| `PL_00_Master_Ingestion_Orchestrator` | Reads active source objects and routes each object to the correct child pipeline. |
| `PL_01_SQL_Incremental_Ingestion` | Extracts SQL Server rows incrementally using a datetime watermark and writes Parquet to ADLS Gen2. |
| `PL_02_File_Ingestion` | Copies CSV and JSON files from ADLS `landing` to ADLS `bronze`. |

---

## Build Sequence

The ADF build followed this order:

```text
1. Configure Self-hosted Integration Runtime.
2. Create SQL Server source linked service.
3. Create SQL Server control linked service.
4. Create ADLS Gen2 linked service.
5. Create dynamic SQL Server dataset.
6. Create dynamic ADLS Parquet dataset.
7. Create dynamic ADLS CSV dataset.
8. Create dynamic ADLS JSON dataset.
9. Build SQL incremental ingestion pipeline.
10. Build file ingestion pipeline.
11. Build master orchestrator pipeline.
12. Validate SQL source route.
13. Validate file source route.
14. Validate operational scenarios.
```

---

## 1. `PL_01_SQL_Incremental_Ingestion`

### Purpose

`PL_01_SQL_Incremental_Ingestion` performs incremental extraction from a single SQL Server source object.

It receives a `SourceObjectId`, reads metadata from control tables, calculates the current high watermark, copies eligible rows to ADLS Gen2, and updates the stored watermark only after a successful copy.

### Parameters

| Parameter | Type | Purpose |
|---|---|---|
| `SourceObjectId` | Integer | Identifies the SQL source object to process. |
| `PipelineRunId` | String | Logical run identifier used for control logging and evidence grouping. |

---

## SQL Pipeline Activity Flow

```text
ACT_Lookup_Source_Config
    ↓
ACT_Lookup_Current_High_Watermark
    ↓
ACT_Lookup_Start_Ingestion_Run
    ↓
ACT_Copy_SQL_To_ADLS
    ├── Success → ACT_Lookup_Complete_Ingestion_Run → ACT_SP_Update_Watermark
    └── Failure → ACT_SP_Fail_Ingestion_Run
```

---

## SQL Pipeline Activities

| Activity | Type | Purpose |
|---|---|---|
| `ACT_Lookup_Source_Config` | Lookup | Reads metadata for the selected source object. |
| `ACT_Lookup_Current_High_Watermark` | Lookup | Captures the current `MAX(UpdatedAt)` before copy. |
| `ACT_Lookup_Start_Ingestion_Run` | Lookup | Creates a `Started` run record in `ctl.IngestionRun`. |
| `ACT_Copy_SQL_To_ADLS` | Copy Activity | Copies eligible SQL rows to ADLS Gen2 as Parquet. |
| `ACT_Lookup_Complete_Ingestion_Run` | Lookup | Marks the run as `Succeeded` and records output metrics. |
| `ACT_SP_Update_Watermark` | Stored Procedure | Updates `ctl.SourceObject.LastWatermarkValue` and inserts `ctl.WatermarkHistory`. |
| `ACT_SP_Fail_Ingestion_Run` | Stored Procedure | Marks the run as `Failed` and stores the error message. |

---

## SQL Incremental Query Pattern

The SQL Copy Activity uses a dynamic query based on control metadata.

Core extraction pattern:

```sql
WHERE UpdatedAt > @LastWatermarkValue
  AND UpdatedAt <= @CurrentHighWatermarkValue
```

The lower bound is exclusive to avoid reprocessing rows already captured by previous successful runs.

The upper bound is inclusive to include all rows up to the high watermark captured before the copy started.

---

## SQL Output Path

SQL extraction outputs are written to ADLS Gen2 using this pattern:

```text
bronze/sqlserver/<source_system>/<schema>/<table>/load_date=YYYY-MM-DD/run_id=<run_id>/<table>.parquet
```

Example:

```text
bronze/sqlserver/sales_local/dbo/orders/load_date=2026-05-15/run_id=manual-retry-after-failure/orders.parquet
```

---

## SQL Success Path

When the SQL copy succeeds:

1. A started run already exists in `ctl.IngestionRun`.
2. The Copy Activity writes rows to ADLS Gen2.
3. The complete activity marks the run as `Succeeded`.
4. Row metrics and destination folder are recorded.
5. The stored procedure activity updates the watermark.
6. A watermark history record is inserted.

Expected successful run behavior:

| Field | Expected Value |
|---|---|
| `Status` | `Succeeded` |
| `RowsRead` | Number of rows read by Copy Activity. |
| `RowsCopied` | Number of rows copied to ADLS. |
| `NewWatermarkValue` | Current high watermark for the run. |
| `WatermarkHistory` | One record inserted. |

---

## SQL Failure Path

When the SQL copy fails:

1. The failure dependency path runs.
2. `ACT_SP_Fail_Ingestion_Run` marks the run as `Failed`.
3. The error message is recorded.
4. `NewWatermarkValue` remains `NULL`.
5. `ctl.SourceObject.LastWatermarkValue` is not updated.
6. No watermark history record is inserted.

Expected failed run behavior:

| Field | Expected Value |
|---|---|
| `Status` | `Failed` |
| `NewWatermarkValue` | `NULL` |
| `WatermarkHistory_Count` | `0` |
| Pending rows | Still eligible for retry. |

---

## 2. `PL_02_File_Ingestion`

### Purpose

`PL_02_File_Ingestion` processes file-based source objects.

It reads file metadata from control tables and copies CSV or JSON files from ADLS Gen2 `landing` into ADLS Gen2 `bronze`.

### Parameters

| Parameter | Type | Purpose |
|---|---|---|
| `SourceObjectId` | Integer | Identifies the file source object to process. |
| `PipelineRunId` | String | Logical run identifier used for control logging and evidence grouping. |

---

## File Pipeline Activity Flow

```text
ACT_Lookup_File_Source_Config
    ↓
ACT_Lookup_Start_File_Ingestion_Run
    ↓
ACT_IF_File_Is_CSV
    ├── True  → ACT_Copy_CSV_To_Bronze
    │              ├── Success → ACT_Lookup_Complete_CSV_Ingestion_Run
    │              └── Failure → ACT_SP_Fail_CSV_Ingestion_Run
    │
    └── False → ACT_Copy_JSON_To_Bronze
                   ├── Success → ACT_Lookup_Complete_JSON_Ingestion_Run
                   └── Failure → ACT_SP_Fail_JSON_Ingestion_Run
```

---

## File Pipeline Activities

| Activity | Type | Purpose |
|---|---|---|
| `ACT_Lookup_File_Source_Config` | Lookup | Reads file source configuration. |
| `ACT_Lookup_Start_File_Ingestion_Run` | Lookup | Creates a `Started` run record. |
| `ACT_IF_File_Is_CSV` | If Condition | Routes file processing based on `SourceType`. |
| `ACT_Copy_CSV_To_Bronze` | Copy Activity | Copies CSV files from `landing` to `bronze`. |
| `ACT_Copy_JSON_To_Bronze` | Copy Activity | Copies JSON files from `landing` to `bronze`. |
| `ACT_Lookup_Complete_CSV_Ingestion_Run` | Lookup | Marks CSV ingestion as `Succeeded`. |
| `ACT_Lookup_Complete_JSON_Ingestion_Run` | Lookup | Marks JSON ingestion as `Succeeded`. |
| `ACT_SP_Fail_CSV_Ingestion_Run` | Stored Procedure | Marks CSV ingestion as `Failed`. |
| `ACT_SP_Fail_JSON_Ingestion_Run` | Stored Procedure | Marks JSON ingestion as `Failed`. |

---

## File Routing Logic

The file pipeline uses `SourceType` to choose the correct branch.

```text
If SourceType = CSV_FILE:
    Use CSV branch

If SourceType = JSON_FILE:
    Use JSON branch
```

Validated file source types:

| SourceType | Result |
|---|---|
| `CSV_FILE` | Successfully copied from `landing` to `bronze`. |
| `JSON_FILE` | Successfully copied from `landing` to `bronze`. |

---

## File Output Path

File outputs are written using this pattern:

```text
bronze/files/<format>/<source_object>/load_date=YYYY-MM-DD/run_id=<run_id>/<file_name>
```

Examples:

```text
bronze/files/csv/currency_rates/load_date=2026-05-14/run_id=<run_id>/currency_rates.csv
bronze/files/json/source_system_metadata/load_date=2026-05-14/run_id=<run_id>/source_system_metadata.json
```

---

## File Ingestion Logging

File ingestion runs are also logged in:

```text
ctl.IngestionRun
```

For file ingestion, row counts may not always be available in the same way as SQL Copy Activity output metrics.

The framework still records:

- Pipeline run ID
- Source object
- Source type
- Status
- Destination container
- Destination folder
- Start and end time
- Error message when applicable

---

## 3. `PL_00_Master_Ingestion_Orchestrator`

### Purpose

`PL_00_Master_Ingestion_Orchestrator` coordinates execution across active source objects.

It reads active objects from control metadata and routes each object to the correct child pipeline.

### Parameters

| Parameter | Type | Purpose |
|---|---|---|
| `RunMode` | String | Controls whether SQL sources, file sources, or all sources are selected. |
| `SourceSystemName` | String | Optional source system filter. |

---

## Master Pipeline Activity Flow

```text
ACT_Lookup_Active_Source_Objects
    ↓
ACT_ForEach_Source_Object
    ↓
ACT_IF_Source_Is_SQL
    ├── True  → ACT_Execute_SQL_Ingestion
    └── False → ACT_Execute_File_Ingestion
```

---

## Master Pipeline Activities

| Activity | Type | Purpose |
|---|---|---|
| `ACT_Lookup_Active_Source_Objects` | Lookup | Reads active source objects from control metadata. |
| `ACT_ForEach_Source_Object` | ForEach | Iterates through active source objects. |
| `ACT_IF_Source_Is_SQL` | If Condition | Routes execution based on `SourceType`. |
| `ACT_Execute_SQL_Ingestion` | Execute Pipeline | Executes `PL_01_SQL_Incremental_Ingestion`. |
| `ACT_Execute_File_Ingestion` | Execute Pipeline | Executes `PL_02_File_Ingestion`. |

---

## Master Source Selection

The master pipeline calls:

```text
ctl.usp_GetActiveSourceObjects
```

The stored procedure supports source filtering through:

| Parameter | Purpose |
|---|---|
| `RunMode` | Allows modes such as SQL-only or file-only execution. |
| `SourceSystemName` | Allows filtering by source system. |

Validated execution modes:

| Mode | Result |
|---|---|
| `FILES_ONLY` | Routed file sources to `PL_02_File_Ingestion`. |
| `SQL_ONLY` | Routed SQL sources to `PL_01_SQL_Incremental_Ingestion`. |

---

## Why `FILES_ONLY` and `SQL_ONLY` Were Used for Evidence

Both orchestration branches were validated separately.

This made the evidence clearer and avoided a long, noisy `ALL` execution screenshot.

The separate runs prove:

| Evidence | Proves |
|---|---|
| `FILES_ONLY` | Master can route CSV and JSON objects to file ingestion. |
| `SQL_ONLY` | Master can route SQL table objects to SQL ingestion. |

---

## Parameter Passing Between Pipelines

The master pipeline passes values into child pipelines.

Example:

```text
SourceObjectId = @item().SourceObjectId
PipelineRunId  = @pipeline().RunId
```

Inside the child pipeline, these are referenced as:

```text
pipeline().parameters.SourceObjectId
pipeline().parameters.PipelineRunId
```

This avoids using `@item()` inside child pipelines, where it is not available.

---

## Build Issues Resolved

Several implementation issues were resolved during the build.

| Issue | Resolution |
|---|---|
| Dynamic dataset preview failed for non-existing paths | Confirmed as expected behavior when parameterized paths do not exist yet. |
| Parquet copy failed due to missing Java Runtime on SHIR machine | Installed/configured Java Runtime and reran successfully. |
| Stored procedure calls through Lookup caused query issues for updates | Used Stored Procedure activity where appropriate. |
| Renamed activities left broken expressions | Updated expressions to reference the new activity names. |
| `@item()` was incorrectly passed into a child pipeline context | Corrected parameter passing through Execute Pipeline activity. |
| File Copy output did not expose `rowsRead`/`rowsCopied` | Adjusted file ingestion logging expectations. |

---

## Evidence Captured

The pipeline build was validated through evidence screenshots.

| Evidence | Description |
|---|---|
| `48_sql_copy_to_adls_success.png` | SQL Server to ADLS Copy Activity succeeded. |
| `49_sql_pipeline_complete_and_watermark_success.png` | SQL pipeline completed and watermark updated. |
| `50_sql_pipeline_failure_path_configured.png` | SQL failure path configured. |
| `51_file_pipeline_csv_copy_and_control_success.png` | CSV file pipeline succeeded. |
| `52_file_pipeline_json_copy_and_control_success.png` | JSON file pipeline succeeded. |
| `53_master_orchestrator_files_success.png` | Master orchestrator routed file sources successfully. |
| `54_master_orchestrator_sql_success.png` | Master orchestrator routed SQL sources successfully. |
| `55_operational_control_summary_after_master.png` | Control summary validated master orchestration. |

---

## Pipeline Build Result

The build completed successfully.

Implemented and validated:

```text
PL_00_Master_Ingestion_Orchestrator
PL_01_SQL_Incremental_Ingestion
PL_02_File_Ingestion
```

The implementation demonstrated:

- Dynamic source discovery
- SQL and file routing
- Parameterized execution
- SQL incremental extraction
- ADLS Gen2 output generation
- File landing-to-bronze movement
- Watermark updates
- Failure logging
- Retry-safe behavior

---

## Design Value

The pipeline build demonstrates practical Azure Data Factory engineering skills:

- Master/child orchestration
- Dynamic datasets
- Metadata-driven routing
- SQL Server to ADLS ingestion
- CSV and JSON ingestion
- Stored procedure integration
- Watermark-safe incremental loading
- Operational logging
- Failure and retry readiness
- Evidence-based validation

This implementation provides the working core of the ADF Incremental Ingestion Framework.