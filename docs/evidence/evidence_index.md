# Evidence Index

This document lists the main implementation and validation evidence for the `azure-adf-incremental-ingestion-framework` project.

The evidence demonstrates Azure resource setup, ADF connectivity, dynamic datasets, SQL incremental ingestion, file ingestion, orchestration, operational validation, failure handling, and retry behavior.

---

## 1. Azure Resources and Connectivity

| Evidence | Description |
|---|---|
| `35_adls_containers_created.png` | ADLS Gen2 containers created for landing, bronze, rejected, metadata, and evidence zones. |
| `40_sql_linked_service_success.png` | SQL Server source linked service connection through Self-hosted Integration Runtime. |
| `41_control_sql_linked_service_success.png` | SQL Server control metadata linked service connection through Self-hosted Integration Runtime. |
| `42_adls_linked_service_success.png` | ADLS Gen2 linked service connection using managed identity. |

---

## 2. Dynamic Datasets

| Evidence | Description |
|---|---|
| `43_dynamic_sqlserver_dataset.png` | Parameterized SQL Server dataset using schema and table parameters. |
| `44_dynamic_adls_parquet_dataset.png` | Parameterized ADLS Parquet dataset for SQL extraction outputs. |
| `45_dynamic_adls_csv_dataset.png` | Parameterized ADLS CSV dataset with preview validation. |
| `46_dynamic_adls_json_dataset.png` | Parameterized ADLS JSON dataset with preview validation. |

---

## 3. SQL Incremental Ingestion Pipeline

| Evidence | Description |
|---|---|
| `48_sql_copy_to_adls_success.png` | SQL Server to ADLS Gen2 Parquet copy activity success. |
| `49_sql_pipeline_complete_and_watermark_success.png` | SQL pipeline completed successfully and watermark was updated. |
| `50_sql_pipeline_failure_path_configured.png` | SQL pipeline failure path configured to log failed ingestion runs. |

---

## 4. File Ingestion Pipeline

| Evidence | Description |
|---|---|
| `51_file_pipeline_csv_copy_and_control_success.png` | CSV file ingestion from landing to bronze with control metadata validation. |
| `52_file_pipeline_json_copy_and_control_success.png` | JSON file ingestion from landing to bronze with control metadata validation. |

---

## 5. Master Orchestrator

| Evidence | Description |
|---|---|
| `53_master_orchestrator_files_success.png` | Master orchestrator successfully routed file sources to the file ingestion pipeline. |
| `54_master_orchestrator_sql_success.png` | Master orchestrator successfully routed SQL sources to the SQL incremental ingestion pipeline. |
| `55_operational_control_summary_after_master.png` | Control metadata summary after master orchestrator execution. |

---

## 6. Incremental Scenarios

| Evidence | Description |
|---|---|
| `56_incremental_insert_source_changes_created.png` | Incremental insert source changes created. |
| `57_incremental_insert_ingestion_success.png` | Incremental insert ingestion succeeded with expected rows copied. |
| `59_incremental_update_ingestion_success.png` | Incremental update ingestion succeeded with expected rows copied and business values updated. |
| `60_refund_source_changes_created.png` | Refund source changes created for Orders and Payments. |
| `61_refund_ingestion_success.png` | Refund ingestion succeeded and watermarks advanced correctly. |
| `62_empty_run_validation_success.png` | Empty-run readiness validation passed with zero eligible rows. |

---

## 7. Failure and Retry Validation

| Evidence | Description |
|---|---|
| `63_failed_run_pending_order_change_created.png` | Pending Orders change created before controlled failure test. |
| `64_controlled_failed_run_validation.png` | Failed run was logged, watermark did not advance, and row remained eligible for retry. |
| `65_retry_after_failure_success.png` | Retry after failure succeeded and watermark advanced correctly. |
| `66_operational_validation_final_summary.png` | Final operational validation summary across all major scenarios. |

---

## Evidence Notes

- Screenshots were reviewed before inclusion to avoid exposing secrets or sensitive connection details.
- Evidence is intended to support portfolio review and technical walkthroughs.
- Some implementation screenshots were intentionally omitted to keep the public repository concise and recruiter-friendly.