# ADF Connectivity Setup

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the Azure Data Factory connectivity setup used by the ADF Incremental Ingestion Framework.

The framework requires Azure Data Factory to connect to:

- Local SQL Server source tables
- Local SQL Server control metadata
- Azure Data Lake Storage Gen2

The connectivity setup validates that ADF can move data from a local SQL Server environment into ADLS Gen2 and can also read/write operational metadata through control tables.

---

## Connectivity Overview

The project uses the following connectivity pattern:

```text
SQL Server Local
    ↓
Self-hosted Integration Runtime
    ↓
Azure Data Factory
    ↓
Azure Data Lake Storage Gen2
```

For file-based ingestion, the pattern is:

```text
ADLS Gen2 landing
    ↓
Azure Data Factory
    ↓
ADLS Gen2 bronze
```

---

## Integration Runtimes

Two integration runtime patterns were used:

| Integration Runtime | Purpose |
|---|---|
| `shir-local-sql-dev` | Self-hosted Integration Runtime used to connect ADF to local SQL Server. |
| `AutoResolveIntegrationRuntime` | Default Azure integration runtime used for cloud-to-cloud ADLS operations. |

---

## Self-hosted Integration Runtime

### Name

```text
shir-local-sql-dev
```

### Purpose

The Self-hosted Integration Runtime allows Azure Data Factory to connect to the local SQL Server environment.

This is required because SQL Server is running locally and is not directly accessible from Azure.

### Validated State

The Self-hosted Integration Runtime was successfully connected to Azure Data Factory.

The Integration Runtime Configuration Manager showed the node connected to the cloud service.

### Why SHIR Matters

This project demonstrates a hybrid ingestion pattern.

In real-world environments, many organizations still ingest data from:

- On-premises SQL Server
- Local network databases
- Private network systems
- Legacy operational systems

SHIR is the Azure Data Factory mechanism used to bridge that local/private environment with Azure pipelines.

---

## Java Runtime Requirement for Parquet

During SQL Server to ADLS Parquet ingestion, the Self-hosted Integration Runtime required Java Runtime support.

The Copy Activity failed initially because Java was not available for Parquet handling.

After installing Java and configuring the required system environment variables, the SQL to Parquet copy succeeded.

This is an important operational lesson for SHIR-based Parquet workloads.

---

## Linked Services

The project uses three main linked services.

| Linked Service | Type | Integration Runtime | Purpose |
|---|---|---|---|
| `LS_SQLSERVER_LOCAL_SHIR` | SQL Server | `shir-local-sql-dev` | Connects to SQL Server source tables. |
| `LS_CONTROL_SQLSERVER_LOCAL` | SQL Server | `shir-local-sql-dev` | Connects to SQL Server control metadata. |
| `LS_ADLSGEN2_DEV` | Azure Data Lake Storage Gen2 | `AutoResolveIntegrationRuntime` | Connects to ADLS Gen2 using managed identity. |

---

## SQL Server Source Linked Service

### Name

```text
LS_SQLSERVER_LOCAL_SHIR
```

### Purpose

This linked service connects Azure Data Factory to the SQL Server source tables.

It is used by datasets and activities that read from:

```text
dbo.Customers
dbo.Products
dbo.Orders
dbo.OrderItems
dbo.Payments
```

### Connection Pattern

```text
ADF
    ↓
Self-hosted Integration Runtime
    ↓
SQL Server local
    ↓
dbo source tables
```

### Evidence

The linked service connection was validated successfully.

Relevant evidence:

```text
40_sql_linked_service_success.png
```

---

## SQL Server Control Metadata Linked Service

### Name

```text
LS_CONTROL_SQLSERVER_LOCAL
```

### Purpose

This linked service connects Azure Data Factory to the SQL Server control metadata layer.

It is used by Lookup and Stored Procedure activities that interact with:

```text
ctl.SourceObject
ctl.FileSourceConfig
ctl.IngestionRun
ctl.IngestionRunStep
ctl.WatermarkHistory
```

### Why Separate It from the Source Linked Service

Although the MVP uses the same local SQL Server database for both source data and control metadata, separate linked services were used to create a clearer logical separation:

| Linked Service | Responsibility |
|---|---|
| `LS_SQLSERVER_LOCAL_SHIR` | Source data access. |
| `LS_CONTROL_SQLSERVER_LOCAL` | Operational metadata access. |

This makes the design easier to explain and closer to enterprise patterns where source systems and metadata/control stores may be separate.

### Evidence

The control metadata linked service connection was validated successfully.

Relevant evidence:

```text
41_control_sql_linked_service_success.png
```

---

## ADLS Gen2 Linked Service

### Name

```text
LS_ADLSGEN2_DEV
```

### Purpose

This linked service connects Azure Data Factory to Azure Data Lake Storage Gen2.

It is used by:

- SQL Parquet sink datasets
- CSV source/sink datasets
- JSON source/sink datasets
- File ingestion from `landing` to `bronze`

### Authentication

The linked service uses:

```text
System-assigned managed identity
```

This avoids storing storage keys or connection strings in the repository.

### Connection Pattern

```text
ADF
    ↓
Managed Identity
    ↓
ADLS Gen2
```

### Evidence

The ADLS Gen2 linked service connection was validated successfully.

Relevant evidence:

```text
42_adls_linked_service_success.png
```

---

## SQL Login for ADF

The local SQL Server environment includes a SQL login/user for ADF connectivity.

The public repository includes the script:

```text
sql/ddl/06_create_adf_sql_login.sql
```

The script uses a placeholder:

```text
REPLACE_WITH_STRONG_LOCAL_PASSWORD
```

Before running the script locally, replace the placeholder with a secure password.

Do not commit real passwords to the repository.

---

## Linked Service Security Notes

The public repository does not include:

- Real SQL passwords
- SQL connection strings
- Storage account keys
- SAS tokens
- Local machine names
- Personal credentials

Screenshots included in evidence were reviewed to avoid exposing sensitive configuration details.

---

## Connectivity Validation Sequence

The connectivity setup was validated in this order:

```text
1. Create Azure Data Factory.
2. Install and register Self-hosted Integration Runtime.
3. Confirm SHIR node is connected to ADF.
4. Create SQL Server source linked service.
5. Test SQL Server source connection.
6. Create SQL Server control metadata linked service.
7. Test SQL Server control metadata connection.
8. Create ADLS Gen2 linked service.
9. Test ADLS Gen2 connection.
10. Validate dynamic datasets using the linked services.
```

---

## Why Connectivity Was Split This Way

The design intentionally separates connectivity responsibilities:

| Concern | Implementation |
|---|---|
| Local source data access | SQL Server linked service through SHIR. |
| Control metadata access | Separate SQL Server control linked service through SHIR. |
| Cloud storage access | ADLS Gen2 linked service through managed identity. |
| Cloud-to-cloud file movement | AutoResolveIntegrationRuntime. |
| Local-to-cloud data movement | Self-hosted Integration Runtime. |

This makes the framework easier to reason about and troubleshoot.

---

## Operational Issues Encountered

### SHIR Setup

Self-hosted Integration Runtime needed to be installed locally and connected to the ADF instance.

### Java Runtime for Parquet

A SQL to Parquet copy failed until Java Runtime was installed and configured on the SHIR machine.

### Dynamic Dataset Validation

Parameterized datasets initially returned errors when preview parameters pointed to non-existing paths.

This was expected during configuration because dynamic paths are only valid when runtime parameters resolve to actual files/folders.

---

## Evidence References

| Evidence | Description |
|---|---|
| `40_sql_linked_service_success.png` | SQL Server source linked service test succeeded. |
| `41_control_sql_linked_service_success.png` | SQL Server control metadata linked service test succeeded. |
| `42_adls_linked_service_success.png` | ADLS Gen2 linked service test succeeded. |
| `48_sql_copy_to_adls_success.png` | SQL Server to ADLS copy succeeded after connectivity and runtime configuration. |
| `51_file_pipeline_csv_copy_and_control_success.png` | CSV file ingestion succeeded through ADLS connectivity. |
| `52_file_pipeline_json_copy_and_control_success.png` | JSON file ingestion succeeded through ADLS connectivity. |

---

## Design Value

The connectivity setup demonstrates:

- Hybrid local-to-cloud data ingestion
- Self-hosted Integration Runtime configuration
- SQL Server source connectivity
- SQL Server metadata/control connectivity
- ADLS Gen2 managed identity connectivity
- Separation of source and control concerns
- Public-safe credential handling
- Runtime troubleshooting and operational readiness

This setup provides the connectivity foundation required for the metadata-driven ingestion framework.