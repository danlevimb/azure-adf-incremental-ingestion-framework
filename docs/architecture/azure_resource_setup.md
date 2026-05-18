# Azure Resource Setup

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the Azure resources created for the ADF Incremental Ingestion Framework.

The Azure environment provides the cloud orchestration and storage layer for the project.

The main Azure services used are:

- Azure Resource Group
- Azure Data Factory
- Azure Data Lake Storage Gen2

The setup was intentionally kept cost-aware and focused on the minimum services required to demonstrate a professional incremental ingestion framework.

---

## Azure Resource Group

The project resources were grouped under a dedicated Azure Resource Group:

```text
rg-adf-incremental-ingestion-dev
```

The resource group acts as the logical container for all Azure resources used by the project.

---

## Resource Group Contents

The development resource group contains:

| Resource | Type | Purpose |
|---|---|---|
| `adf-incremental-ingestion-dev` | Azure Data Factory | Orchestrates SQL and file-based ingestion pipelines. |
| `stgadfingdan01` | Storage Account | ADLS Gen2 storage account used for landing, bronze, metadata, rejected, and evidence zones. |

---

## Naming Notes

The storage account name was shortened to comply with Azure Storage naming constraints.

Azure Storage account names must be globally unique and follow strict length and character rules.

The selected development storage account was:

```text
stgadfingdan01
```

---

## Azure Data Factory

The Azure Data Factory instance used by the project is:

```text
adf-incremental-ingestion-dev
```

### Role

Azure Data Factory is responsible for:

- Connecting to local SQL Server through Self-hosted Integration Runtime
- Connecting to ADLS Gen2
- Running metadata-driven pipelines
- Executing SQL incremental ingestion
- Executing CSV and JSON file ingestion
- Running master orchestration
- Logging operational results through SQL control tables
- Supporting monitoring and debugging evidence

---

## Azure Data Lake Storage Gen2

The ADLS Gen2 storage account used by the project is:

```text
stgadfingdan01
```

### Role

ADLS Gen2 is used as the landing and bronze storage layer.

It stores:

- Source CSV files
- Source JSON files
- SQL extraction outputs in Parquet format
- Copied CSV/JSON outputs
- Future metadata or evidence artifacts if needed

---

## ADLS Gen2 Containers

The following containers were created:

| Container | Purpose |
|---|---|
| `landing` | Input area for source CSV and JSON files. |
| `bronze` | Target area for SQL Parquet outputs and copied file-based sources. |
| `rejected` | Reserved for rejected files or future validation failures. |
| `metadata` | Reserved for operational metadata exports or future reporting. |
| `evidence` | Reserved for evidence artifacts if needed. |

---

## Container Creation Script

The public repository includes a parameterized script for container creation:

```text
scripts/azure/01_create_adls_containers.ps1
```

The script is safe for public repository use because it uses placeholders for resource-specific values.

Example variables:

```powershell
$resourceGroupName = "<RESOURCE_GROUP_NAME>"
$storageAccountName = "<STORAGE_ACCOUNT_NAME>"
```

The script creates:

```powershell
$containers = @(
    "landing",
    "bronze",
    "rejected",
    "metadata",
    "evidence"
)
```

---

## Authentication Strategy

The ADLS Gen2 linked service in Azure Data Factory was configured using:

```text
System-assigned managed identity
```

This avoids storing storage account keys or connection strings directly inside the project documentation or repository.

The public repository does not include:

- Storage account keys
- Connection strings
- Shared access signatures
- Secrets

---

## Cost-Aware Setup

The Azure resource setup was intentionally minimal.

The MVP uses:

| Service | Cost Strategy |
|---|---|
| Azure Data Factory | Used for orchestration and pipeline execution only. |
| ADLS Gen2 | Used as the required cloud data lake target. |
| SQL Server local | Used instead of Azure SQL Database to reduce cloud cost. |
| Self-hosted Integration Runtime | Used to connect ADF to local SQL Server. |

More advanced services such as Azure Databricks, Synapse Dedicated SQL Pool, Microsoft Purview, and full CI/CD infrastructure were intentionally deferred.

---

## Local-to-Cloud Pattern

The project demonstrates a hybrid ingestion pattern:

```text
SQL Server Local
    ↓
Self-hosted Integration Runtime
    ↓
Azure Data Factory
    ↓
Azure Data Lake Storage Gen2
```

This pattern is common when organizations need to ingest data from on-premises or local network systems into cloud storage.

---

## File-Based Pattern

The project also demonstrates cloud-to-cloud file movement:

```text
ADLS Gen2 landing
    ↓
Azure Data Factory
    ↓
ADLS Gen2 bronze
```

This pattern was validated for:

```text
CSV_FILE
JSON_FILE
```

---

## Resource Evidence

The Azure resource setup was validated through evidence screenshots.

| Evidence | Description |
|---|---|
| `35_adls_containers_created.png` | Shows the ADLS Gen2 containers created for the framework. |
| `40_sql_linked_service_success.png` | Shows successful SQL Server source linked service connectivity. |
| `41_control_sql_linked_service_success.png` | Shows successful SQL Server control metadata linked service connectivity. |
| `42_adls_linked_service_success.png` | Shows successful ADLS Gen2 linked service connectivity. |

---

## Resource Cleanup Considerations

The public repository includes a development cleanup script:

```text
scripts/cleanup/01_clean_adls_test_outputs.ps1
```

This script is intended to remove generated test output folders from ADLS Gen2.

It is parameterized and includes a confirmation step before deletion.

The cleanup script should only be used in development or test environments.

---

## What Was Not Included

The MVP does not include:

- Azure Key Vault
- Azure SQL Database
- Azure Monitor dashboards
- Microsoft Purview
- CI/CD deployment pipelines
- Infrastructure as Code

These items are valid future improvements, but they were intentionally excluded from the MVP to keep the project focused on incremental ingestion and operational validation.

---

## Design Value

The Azure resource setup demonstrates:

- Cost-aware Azure project setup
- Dedicated resource grouping
- Azure Data Factory orchestration
- ADLS Gen2 storage organization
- Managed identity usage for ADLS connectivity
- Hybrid local SQL Server ingestion through SHIR
- Public-safe scripting practices
- Separation between implementation resources and documentation evidence

This setup provides the cloud foundation for the metadata-driven ingestion framework.