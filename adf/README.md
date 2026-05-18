# Azure Data Factory Artifacts

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This folder is reserved for Azure Data Factory artifacts.

The project was implemented and validated directly in Azure Data Factory Studio during the MVP phase.

The current repository documents the ADF implementation through architecture documents, pipeline design notes, SQL scripts, evidence screenshots, and operational validation results.

ADF Git integration is planned as a future improvement so that ADF-generated JSON artifacts can be version-controlled cleanly.

---

## ADF Implementation Status

The following ADF assets were created and validated in Azure Data Factory Studio:

## Pipelines

| Pipeline                              | Purpose                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------ |
| `PL_00_Master_Ingestion_Orchestrator` | Reads active source objects and routes execution to SQL or file ingestion child pipelines. |
| `PL_01_SQL_Incremental_Ingestion`     | Performs incremental SQL Server extraction into ADLS Gen2 Parquet using watermarks.        |
| `PL_02_File_Ingestion`                | Copies CSV and JSON files from ADLS `landing` to ADLS `bronze`.                            |

---

## Linked Services

| Linked Service               | Purpose                                                                                    |
| ---------------------------- | ------------------------------------------------------------------------------------------ |
| `LS_SQLSERVER_LOCAL_SHIR`    | Connects ADF to local SQL Server source tables through Self-hosted Integration Runtime.    |
| `LS_CONTROL_SQLSERVER_LOCAL` | Connects ADF to local SQL Server control metadata through Self-hosted Integration Runtime. |
| `LS_ADLSGEN2_DEV`            | Connects ADF to Azure Data Lake Storage Gen2 using managed identity.                       |

---

## Datasets

| Dataset                      | Purpose                                                            |
| ---------------------------- | ------------------------------------------------------------------ |
| `DS_SQLSERVER_TABLE_DYNAMIC` | Dynamic SQL Server table access using schema and table parameters. |
| `DS_ADLS_PARQUET_DYNAMIC`    | Dynamic ADLS Gen2 Parquet output paths for SQL extraction.         |
| `DS_ADLS_CSV_DYNAMIC`        | Dynamic CSV file source and sink paths.                            |
| `DS_ADLS_JSON_DYNAMIC`       | Dynamic JSON file source and sink paths.                           |

---

## Integration Runtimes

| Integration Runtime             | Purpose                                                                    |
| ------------------------------- | -------------------------------------------------------------------------- |
| `shir-local-sql-dev`            | Self-hosted Integration Runtime used to connect ADF to local SQL Server.   |
| `AutoResolveIntegrationRuntime` | Default Azure integration runtime used for ADLS cloud-to-cloud operations. |

---

## Why ADF JSON Artifacts Are Not Included Yet

During the MVP build, the focus was to:

1. Design the ingestion framework.
2. Build and validate ADF pipelines.
3. Capture execution evidence.
4. Validate SQL, CSV, JSON, orchestration, failure, and retry behavior.
5. Package the public repository with documentation, scripts, sample data, and evidence.

ADF Git integration was intentionally deferred until after the public repository was created and organized.

This avoids mixing early design work with ADF-generated factory structure before the repository was ready.

---

## Planned ADF Git Integration

A future improvement is to connect Azure Data Factory to this GitHub repository.

Once ADF Git integration is enabled, this folder or the ADF-generated structure should include version-controlled artifacts such as:

* Pipelines
* Datasets
* Linked services
* Integration runtime references
* Factory configuration files
* Trigger definitions if added later

---

## Expected Future Structure

ADF Git integration may generate a structure similar to:

```text
adf/
├── pipeline/
├── dataset/
├── linkedService/
├── integrationRuntime/
├── trigger/
└── factory/
```

The exact structure may depend on how Azure Data Factory exports or syncs artifacts through Git integration.

---

## Current Documentation Coverage

Although ADF JSON artifacts are not included yet, the current repository documents the implementation through:

| Document                                        | Purpose                                                      |
| ----------------------------------------------- | ------------------------------------------------------------ |
| `docs/architecture/adf_pipeline_design.md`      | Explains master, SQL, and file pipeline design.              |
| `docs/implementation/pipeline_build.md`         | Describes the pipeline build process and activity structure. |
| `docs/implementation/dynamic_datasets.md`       | Documents dynamic dataset configuration.                     |
| `docs/implementation/adf_connectivity_setup.md` | Documents linked services and integration runtimes.          |
| `docs/operations/operational_validation.md`     | Summarizes operational validation results.                   |
| `docs/evidence/evidence_index.md`               | Lists screenshots proving ADF execution and validation.      |

---

## Validated ADF Behavior

The ADF implementation was validated through:

* SQL Server to ADLS Gen2 Parquet copy
* Watermark update after successful SQL ingestion
* Failure path logging
* CSV file ingestion
* JSON file ingestion
* Master orchestrator file routing
* Master orchestrator SQL routing
* Incremental insert scenario
* Incremental update scenario
* Refund scenario
* Empty run validation
* Controlled failed run
* Retry after failure
* Final operational validation summary

---

## Evidence References

Important evidence related to ADF assets:

| Evidence                                             | Description                                    |
| ---------------------------------------------------- | ---------------------------------------------- |
| `48_sql_copy_to_adls_success.png`                    | SQL Copy Activity succeeded.                   |
| `49_sql_pipeline_complete_and_watermark_success.png` | SQL pipeline completed and watermark advanced. |
| `50_sql_pipeline_failure_path_configured.png`        | Failure path configured.                       |
| `51_file_pipeline_csv_copy_and_control_success.png`  | CSV ingestion pipeline succeeded.              |
| `52_file_pipeline_json_copy_and_control_success.png` | JSON ingestion pipeline succeeded.             |
| `53_master_orchestrator_files_success.png`           | Master orchestrator routed file sources.       |
| `54_master_orchestrator_sql_success.png`             | Master orchestrator routed SQL sources.        |
| `66_operational_validation_final_summary.png`        | Final operational validation summary.          |

---

## Future Work

Planned improvements for ADF artifact management:

1. Enable ADF Git integration.
2. Commit ADF-generated JSON artifacts.
3. Add trigger definitions.
4. Add environment parameterization notes.
5. Add deployment guidance.
6. Add CI/CD strategy in a future production-readiness phase.

---

## Conclusion

This folder currently documents the ADF artifact strategy and reserves a location for future ADF Git integration outputs.

The MVP implementation is fully documented through repository documentation and evidence screenshots, while ADF JSON artifact versioning remains a planned enhancement.
