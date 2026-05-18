# Monitoring and Evidence

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the monitoring and evidence strategy used for the ADF Incremental Ingestion Framework.

The goal is to prove that the framework was not only built, but also validated through observable execution results.

The evidence demonstrates:

* Azure resource setup
* ADF linked service connectivity
* Dynamic dataset configuration
* SQL incremental ingestion
* CSV and JSON file ingestion
* Master orchestration
* Control metadata validation
* Incremental insert/update/refund scenarios
* Empty run readiness
* Controlled failure behavior
* Retry after failure
* Final operational validation summary

---

## Evidence Philosophy

This project follows an evidence-driven documentation approach.

Instead of only describing what the framework should do, the repository includes screenshots and validation outputs showing that the framework actually executed successfully.

Evidence is used to support:

* Technical validation
* Portfolio credibility
* Recruiter-facing review
* Interview explanations
* Troubleshooting traceability
* Operational behavior proof

---

## Evidence Location

Selected public evidence is stored under:

```text
docs/evidence/screenshots/
```

The evidence index is stored at:

```text
docs/evidence/evidence_index.md
```

The evidence index explains what each screenshot proves and how it fits into the project narrative.

---

## Evidence Selection Strategy

Not every screenshot captured during development was included in the public repository.

The selected evidence was chosen to show the most important milestones without overwhelming the reviewer.

The evidence set focuses on:

| Evidence Area         | Purpose                                                      |
| --------------------- | ------------------------------------------------------------ |
| Azure setup           | Prove required cloud resources and containers exist.         |
| Connectivity          | Prove ADF can connect to SQL Server and ADLS Gen2.           |
| Dynamic datasets      | Prove reusable parameterized datasets were configured.       |
| SQL ingestion         | Prove SQL Server rows were copied to ADLS Gen2 as Parquet.   |
| File ingestion        | Prove CSV and JSON files were copied from landing to bronze. |
| Master orchestration  | Prove source routing works for SQL and file sources.         |
| Incremental scenarios | Prove inserts, updates, and refunds are captured.            |
| Empty run validation  | Prove no pending rows remain after successful ingestion.     |
| Failure and retry     | Prove failed runs do not advance watermarks and retry works. |
| Final summary         | Prove all major scenarios passed.                            |

---

## Main Evidence Groups

## 1. Azure Resources and Connectivity

These screenshots prove that the Azure foundation and connectivity were configured correctly.

| Evidence                                    | Description                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------- |
| `35_adls_containers_created.png`            | ADLS Gen2 containers were created for the framework.                            |
| `40_sql_linked_service_success.png`         | SQL Server source linked service connected successfully through SHIR.           |
| `41_control_sql_linked_service_success.png` | SQL Server control metadata linked service connected successfully through SHIR. |
| `42_adls_linked_service_success.png`        | ADLS Gen2 linked service connected successfully using managed identity.         |

---

## 2. Dynamic Dataset Evidence

These screenshots prove that ADF datasets were parameterized and reusable.

| Evidence                              | Description                                                         |
| ------------------------------------- | ------------------------------------------------------------------- |
| `43_dynamic_sqlserver_dataset.png`    | SQL Server dynamic dataset resolved schema and table parameters.    |
| `44_dynamic_adls_parquet_dataset.png` | ADLS Parquet dataset was configured with dynamic output parameters. |
| `45_dynamic_adls_csv_dataset.png`     | ADLS CSV dataset preview succeeded with dynamic parameters.         |
| `46_dynamic_adls_json_dataset.png`    | ADLS JSON dataset preview succeeded with dynamic parameters.        |

---

## 3. SQL Pipeline Evidence

These screenshots prove that SQL incremental ingestion works and updates control metadata correctly.

| Evidence                                             | Description                                                      |
| ---------------------------------------------------- | ---------------------------------------------------------------- |
| `48_sql_copy_to_adls_success.png`                    | SQL Server data was copied to ADLS Gen2 as Parquet.              |
| `49_sql_pipeline_complete_and_watermark_success.png` | SQL pipeline completed successfully and updated watermark state. |
| `50_sql_pipeline_failure_path_configured.png`        | SQL pipeline failure path was configured for failed run logging. |

---

## 4. File Pipeline Evidence

These screenshots prove that CSV and JSON file ingestion works.

| Evidence                                             | Description                                                     |
| ---------------------------------------------------- | --------------------------------------------------------------- |
| `51_file_pipeline_csv_copy_and_control_success.png`  | CSV file ingestion succeeded and control metadata was updated.  |
| `52_file_pipeline_json_copy_and_control_success.png` | JSON file ingestion succeeded and control metadata was updated. |

---

## 5. Master Orchestrator Evidence

These screenshots prove that the master pipeline correctly routes source objects to child pipelines.

| Evidence                                          | Description                                                                       |
| ------------------------------------------------- | --------------------------------------------------------------------------------- |
| `53_master_orchestrator_files_success.png`        | Master orchestrator routed file sources to the file ingestion pipeline.           |
| `54_master_orchestrator_sql_success.png`          | Master orchestrator routed SQL sources to the SQL incremental ingestion pipeline. |
| `55_operational_control_summary_after_master.png` | Control metadata summarized successful master orchestrator results.               |

---

## 6. Incremental Scenario Evidence

These screenshots prove that the framework captures inserts, updates, and business state changes.

| Evidence                                           | Description                                                       |
| -------------------------------------------------- | ----------------------------------------------------------------- |
| `56_incremental_insert_source_changes_created.png` | Controlled insert source changes were created.                    |
| `57_incremental_insert_ingestion_success.png`      | Incremental insert ingestion succeeded with expected rows copied. |
| `59_incremental_update_ingestion_success.png`      | Incremental update ingestion succeeded with expected rows copied. |
| `60_refund_source_changes_created.png`             | Refund source changes were created for Orders and Payments.       |
| `61_refund_ingestion_success.png`                  | Refund ingestion succeeded and watermarks advanced correctly.     |
| `62_empty_run_validation_success.png`              | Empty-run readiness validation passed with zero eligible rows.    |

---

## 7. Failure and Retry Evidence

These screenshots prove that failed runs are handled safely and retries work.

| Evidence                                         | Description                                                             |
| ------------------------------------------------ | ----------------------------------------------------------------------- |
| `63_failed_run_pending_order_change_created.png` | Pending Orders change was created before the controlled failure.        |
| `64_controlled_failed_run_validation.png`        | Failed run was logged and watermark did not advance.                    |
| `65_retry_after_failure_success.png`             | Retry after failure succeeded and watermark advanced correctly.         |
| `66_operational_validation_final_summary.png`    | Final operational validation summary showed all major scenarios passed. |

---

## Monitoring Sources

The project uses several monitoring and validation surfaces.

| Monitoring Source         | Purpose                                                                        |
| ------------------------- | ------------------------------------------------------------------------------ |
| Azure Data Factory Studio | Pipeline debug runs, activity status, Copy Activity metrics, failure messages. |
| ADF activity output       | Rows read, rows copied, files read, files written, bytes read, bytes written.  |
| SQL Server control tables | Run history, status, watermarks, row counts, destination folders, failures.    |
| ADLS Gen2 portal view     | Output file and folder validation.                                             |
| SQL validation queries    | Scenario verification and final operational summaries.                         |

---

## ADF Monitoring

ADF Studio was used to validate:

* Pipeline execution status
* Activity execution status
* Copy Activity metrics
* Linked service connectivity
* Dataset preview behavior
* Failure path behavior
* Execute Pipeline activity routing

Typical ADF statuses reviewed:

```text
Correcto
Error
En curso
```

For the public evidence, screenshots were captured only when the relevant pipeline or activity had reached a meaningful validation state.

---

## Copy Activity Metrics

SQL Copy Activity provided row-level metrics such as:

```text
RowsRead
RowsCopied
```

These were used for SQL incremental validation.

Example expected SQL results:

```text
Customers    RowsRead = 1    RowsCopied = 1
Orders       RowsRead = 1    RowsCopied = 1
OrderItems   RowsRead = 2    RowsCopied = 2
Payments     RowsRead = 1    RowsCopied = 1
```

File Copy Activity provided file-level and byte-level metrics such as:

```text
filesRead
filesWritten
dataRead
dataWritten
```

These were used for CSV and JSON ingestion validation.

---

## Control Table Monitoring

The main operational monitoring table is:

```text
ctl.IngestionRun
```

This table records:

* Logical pipeline run ID
* Source object
* Source type
* Run status
* Old watermark
* Current high watermark
* New watermark
* Rows read
* Rows copied
* Destination container
* Destination folder
* Error message
* Start and end timestamps

---

## Watermark Monitoring

Current watermark state is stored in:

```text
ctl.SourceObject.LastWatermarkValue
```

Watermark movement history is stored in:

```text
ctl.WatermarkHistory
```

These tables were used to prove that:

* Watermarks advanced after successful SQL ingestion.
* Watermarks did not advance after failed ingestion.
* Watermark history was inserted only for successful runs.
* Pending rows remained eligible after failure.

---

## Example Operational Validation Query

A typical validation query filters by logical pipeline run ID.

```sql
SELECT
    PipelineRunId,
    SourceObjectName,
    SourceType,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    NewWatermarkValue,
    RowsRead,
    RowsCopied,
    DestinationContainer,
    DestinationFolder
FROM ctl.IngestionRun
WHERE PipelineRunId = 'manual-incremental-insert'
ORDER BY
    SourceObjectName;
```

This pattern makes scenario evidence clear and repeatable.

---

## Final Summary Query

The final validation summary aggregates major scenario outcomes.

It validates:

* Incremental insert
* Incremental update
* Refund scenario
* Controlled failed run
* Retry after failure

Expected result:

```text
Incremental Insert       PASS
Incremental Update       PASS
Refund Scenario          PASS
Controlled Failed Run    PASS
Retry After Failure      PASS
```

It also validates:

```text
PASS_FAILED_RUN_DID_NOT_ADVANCE_WATERMARK
```

---

## Evidence Naming Convention

Evidence files use a numeric prefix and descriptive name.

Example:

```text
57_incremental_insert_ingestion_success.png
```

This naming convention helps preserve execution order and makes the evidence easier to reference in documentation.

Recommended pattern:

```text
<number>_<topic>_<result>.png
```

Examples:

```text
48_sql_copy_to_adls_success.png
52_file_pipeline_json_copy_and_control_success.png
66_operational_validation_final_summary.png
```

---

## Evidence Quality Rules

Evidence screenshots should follow these rules:

| Rule                                                | Reason                                     |
| --------------------------------------------------- | ------------------------------------------ |
| Show only relevant resultsets or UI panels.         | Keeps evidence readable.                   |
| Avoid exposing secrets or passwords.                | Protects security.                         |
| Avoid exposing local machine details when possible. | Keeps public repo safe.                    |
| Prefer compact combined evidence when useful.       | Reduces screenshot clutter.                |
| Capture operational metrics when available.         | Strengthens technical proof.               |
| Include control metadata validation when possible.  | Proves state changes, not just UI success. |

---

## Public Repository Evidence Scope

The public repository includes selected evidence only.

Some development screenshots were intentionally omitted because they were:

* Redundant
* Too detailed
* Debug-only
* Visually noisy
* Not needed for the final project narrative

The selected evidence tells the project story from setup through final operational validation.

---

## Troubleshooting Evidence

Not every error was included as public evidence.

However, some issues informed the final documentation and future improvement notes.

Examples:

| Issue                                                                 | Documentation Outcome                                                       |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Dynamic sink preview failed when output file did not exist            | Documented as expected ADF behavior.                                        |
| Parquet copy required Java Runtime on SHIR machine                    | Documented in connectivity and lessons learned.                             |
| Lookup activity was not ideal for operational write stored procedures | Stored Procedure activity used where appropriate.                           |
| File copy did not expose row metrics                                  | File ingestion documentation uses file/byte metrics instead of row metrics. |
| Controlled failure intentionally failed Copy Activity                 | Included as evidence because failure was expected and meaningful.           |

---

## Evidence and Interview Value

This evidence set can support interview explanations such as:

* How the framework detects new or changed rows.
* How ADF routes SQL vs file sources.
* How dynamic datasets avoid duplicated configuration.
* How control tables drive ingestion behavior.
* How watermarks are protected during failures.
* How a retry processes pending rows safely.
* How SQL Server local connects to ADF through SHIR.
* How ADLS output paths are tied to run IDs.

---

## Final Evidence Result

The selected evidence supports the conclusion that the framework successfully demonstrated:

* Azure Data Factory orchestration
* SQL Server local incremental ingestion
* ADLS Gen2 output generation
* CSV and JSON file ingestion
* Metadata-driven processing
* Watermark management
* Failure-safe behavior
* Retry readiness
* Operational validation

This makes the project suitable for public portfolio presentation.