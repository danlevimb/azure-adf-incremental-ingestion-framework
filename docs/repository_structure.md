# Repository Structure

This repository contains the implementation, documentation, evidence, scripts, and sample data for the `azure-adf-incremental-ingestion-framework` project.

The project demonstrates a metadata-driven incremental ingestion framework using Azure Data Factory, SQL Server local, Azure Data Lake Storage Gen2, control tables, watermarks, operational logging, and validation evidence.

## Root Structure

| Path | Purpose |
|---|---|
| `README.md` | Main project overview, architecture summary, execution narrative, and portfolio positioning. |
| `docs/` | Technical documentation for architecture, implementation, operations, evidence, and certification alignment. |
| `diagrams/` | Architecture and pipeline flow diagrams. |
| `sql/` | SQL Server scripts for source setup, control metadata, stored procedures, validation queries, and test scenarios. |
| `sample-data/` | CSV and JSON files used for file-based ingestion testing. |
| `scripts/` | Azure CLI / PowerShell scripts for resource setup, sample file upload, and cleanup operations. |
| `adf/` | Placeholder for Azure Data Factory exported artifacts or future Git integration files. |

## Documentation Areas

| Path | Purpose |
|---|---|
| `docs/architecture/` | Architecture, pipeline design, control metadata, watermark strategy, and ADLS folder structure. |
| `docs/implementation/` | Step-by-step implementation documentation. |
| `docs/operations/` | Operational validation, incremental scenarios, failure handling, retry behavior, and monitoring evidence. |
| `docs/evidence/` | Evidence index and screenshots proving the project execution. |

## SQL Areas

| Path | Purpose |
|---|---|
| `sql/ddl/` | Database, schema, source table, and control table creation scripts. |
| `sql/dml/` | Seed data scripts. |
| `sql/stored-procedures/` | Stored procedures used by ADF for metadata lookup, run logging, watermark updates, and failure handling. |
| `sql/validation-queries/` | SQL queries used to validate source data, control metadata, and operational results. |
| `sql/test-scenarios/` | Controlled test scenarios for inserts, updates, refunds, empty runs, failures, and retries. |

## Evidence Strategy

Evidence is organized to support a recruiter-facing review of the project.

Screenshots and terminal outputs should demonstrate:

- Azure resources created
- Self-hosted Integration Runtime connectivity
- Linked services
- Dynamic datasets
- ADF pipelines
- SQL incremental ingestion
- CSV and JSON file ingestion
- Master orchestration
- Incremental insert/update/refund scenarios
- Empty run validation
- Controlled failure behavior
- Retry after failure
- Final operational validation summary

## ADF Artifact Strategy

During the MVP implementation, ADF pipelines and datasets were created and tested directly in Azure Data Factory Studio.

ADF Git integration is planned after public repository creation. Once enabled, ADF-generated JSON artifacts should be committed under the structure produced by Azure Data Factory Git integration.

Until then, the `adf/` folder documents the ADF artifact strategy and serves as the future location for exported or Git-integrated ADF artifacts.