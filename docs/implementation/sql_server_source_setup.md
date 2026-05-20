# SQL Server Source Setup

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the SQL Server local setup used by the ADF Incremental Ingestion Framework.

SQL Server local acts as:

1. The primary source system for SQL table ingestion.
2. The control metadata store for source configuration, ingestion runs, watermarks, and validation state.

The setup is intentionally local and cost-aware, while still demonstrating professional Azure Data Engineering ingestion patterns.

---

## SQL Server Role in the Project

SQL Server local provides the main relational source model for the project.

Azure Data Factory connects to SQL Server through:

```text
Self-hosted Integration Runtime
```

This creates a realistic hybrid ingestion pattern:

```text
SQL Server Local
    ↓
Self-hosted Integration Runtime
    ↓
Azure Data Factory
    ↓
Azure Data Lake Storage Gen2
```

---

## Database

The local SQL Server database used by the project is:

```text
ADF_Ingestion_Source
```

This database contains:

| Schema | Purpose |
|---|---|
| `dbo` | Source business tables. |
| `ctl` | Control metadata tables and stored procedures. |

---

## Source Domain

The source domain is:

```text
Sales order processing
```

This domain was selected because it is easy to understand while still providing enough relational behavior for realistic incremental ingestion scenarios.

The model supports:

- Customers
- Products
- Orders
- Order items
- Payments
- Business updates
- Refund scenarios
- Incremental inserts
- Incremental updates

---

## Source Tables

The source schema includes the following tables:

| Table | Purpose |
|---|---|
| `dbo.Customers` | Customer master data. |
| `dbo.Products` | Product master data. |
| `dbo.Orders` | Sales order header records. |
| `dbo.OrderItems` | Sales order detail lines. |
| `dbo.Payments` | Payment records associated with orders. |

---

## Incremental Timestamp Columns

Each source table includes:

```sql
CreatedAt datetime2(3) NOT NULL
UpdatedAt datetime2(3) NOT NULL
```

The framework uses:

```text
UpdatedAt
```

as the official watermark column for SQL incremental ingestion.

---

## Why `UpdatedAt` Matters

The `UpdatedAt` column allows the pipeline to detect:

| Change Type | Detected By |
|---|---|
| New rows | New row has a recent `UpdatedAt`. |
| Updated rows | Existing row receives a newer `UpdatedAt`. |
| Business state changes | Status changes update `UpdatedAt`. |

This makes `UpdatedAt` useful for both insert and update ingestion scenarios.

---

## SQL Script Inventory

The public repository stores SQL setup scripts under:

```text
sql/
```

Recommended structure:

```text
sql/
├── ddl/
├── dml/
├── stored-procedures/
├── validation-queries/
└── test-scenarios/
```

---

## DDL Scripts

DDL scripts are stored under:

```text
sql/ddl/
```

Expected scripts:

| Script | Purpose |
|---|---|
| `01_create_database.sql` | Creates the `ADF_Ingestion_Source` database. |
| `02_create_source_tables.sql` | Creates the SQL Server source tables under `dbo`. |
| `03_create_control_schema.sql` | Creates the `ctl` schema. |
| `04_create_control_tables.sql` | Creates control metadata tables. |
| `05_create_indexes.sql` | Creates supporting indexes for source and control metadata tables. |
| `06_create_adf_sql_login.sql` | Creates the SQL login/user used by ADF through SHIR. |

---

## DML Scripts

DML scripts are stored under:

```text
sql/dml/
```

Expected scripts:

| Script | Purpose |
|---|---|
| `01_seed_source_data.sql` | Inserts realistic source records into the `dbo` tables. |
| `02_seed_source_objects.sql` | Inserts metadata records into the control tables. |

---

## Stored Procedure Scripts

Stored procedures are stored under:

```text
sql/stored-procedures/
```

Expected scripts:

| Script | Stored Procedure |
|---|---|
| `01_usp_GetActiveSourceObjects.sql` | `ctl.usp_GetActiveSourceObjects` |
| `02_usp_GetSourceObjectConfig.sql` | `ctl.usp_GetSourceObjectConfig` |
| `03_usp_GetCurrentHighWatermark.sql` | `ctl.usp_GetCurrentHighWatermark` |
| `04_usp_StartIngestionRun.sql` | `ctl.usp_StartIngestionRun` |
| `05_usp_CompleteIngestionRun.sql` | `ctl.usp_CompleteIngestionRun` |
| `06_usp_FailIngestionRun.sql` | `ctl.usp_FailIngestionRun` |
| `07_usp_UpdateWatermark.sql` | `ctl.usp_UpdateWatermark` |
| `08_usp_LogIngestionRunStep.sql` | `ctl.usp_LogIngestionRunStep` |

---

## Recommended Execution Order

Run the SQL scripts in this order:

```text
1. sql/ddl/01_create_database.sql
2. sql/ddl/02_create_source_tables.sql
3. sql/ddl/03_create_control_schema.sql
4. sql/ddl/04_create_control_tables.sql
5. sql/ddl/05_create_indexes.sql
6. sql/dml/01_seed_source_data.sql
7. sql/dml/02_seed_source_objects.sql
8. sql/stored-procedures/*.sql
9. sql/ddl/06_create_adf_sql_login.sql
10. sql/validation-queries/*.sql
```

The ADF SQL login script should be reviewed before execution because it contains a password placeholder.

---

## ADF SQL Login Script

The script:

```text
sql/ddl/06_create_adf_sql_login.sql
```

uses a placeholder for the SQL login password:

```text
REPLACE_WITH_STRONG_LOCAL_PASSWORD
```

Before running locally, replace the placeholder with a secure local password.

Do not commit real passwords to the repository.

The public repository intentionally keeps only the placeholder.

---

## Security Notes

This project does not commit:

- Real passwords
- Connection strings
- Storage account keys
- Secrets
- Local configuration files

The `.gitignore` excludes common secret and local configuration patterns.

ADF connections should be configured through Azure Data Factory linked services and local secure configuration, not through committed credentials.

---

## Source Table Relationships

The source model includes relational relationships between sales entities.

Conceptual relationships:

```text
Customers 1 ── many Orders
Orders    1 ── many OrderItems
Orders    1 ── many Payments
Products  1 ── many OrderItems
```

This makes the source model more realistic than independent flat tables.

---

## Source Data Scenarios

The seeded source data supports the following validation scenarios:

| Scenario | Purpose |
|---|---|
| Initial ingestion | Load existing source records using the incremental pattern from the initial low watermark. |
| Incremental insert | Insert new records and validate that only new rows are copied. |
| Incremental update | Update existing records and validate that changed rows are copied. |
| Refund scenario | Update related Orders and Payments records. |
| Empty run validation | Confirm there are no eligible rows after watermarks advance. |
| Controlled failed run | Force failure and validate that watermark does not advance. |
| Retry after failure | Retry pending rows and validate successful watermark advancement. |

---

## Initial Low Watermark

The control metadata starts SQL source objects with:

```text
1900-01-01 00:00:00.000
```

This allows the first run to behave like a full initial load while still using the incremental extraction logic.

---

## Validation Queries

Validation scripts are stored under:

```text
sql/validation-queries/
```

They are used to confirm:

- Source row counts
- Source relationships
- Control metadata records
- Watermark values
- Ingestion run status
- Watermark history
- Scenario outcomes

---

## Test Scenarios

Test scenario scripts are stored under:

```text
sql/test-scenarios/
```

Expected scripts:

| Script | Purpose |
|---|---|
| `01_initial_full_load_baseline.sql` | Validates baseline source and watermark state. |
| `02_incremental_insert_scenario.sql` | Creates controlled insert changes. |
| `03_incremental_update_scenario.sql` | Creates controlled update changes. |
| `04_refund_scenario.sql` | Creates controlled refund changes. |
| `05_empty_run_validation.sql` | Confirms zero eligible rows after successful scenarios. |
| `06_failed_run_setup.sql` | Prepares and restores a controlled failure configuration. |
| `07_retry_after_failure_validation.sql` | Validates retry readiness and retry success conditions. |

---

## Safe Defaults for Scenario Scripts

Scenario scripts are designed with safe defaults.

Scripts that modify source data should keep:

```sql
DECLARE @ExecuteScenario bit = 0;
```

by default.

To execute a scenario locally, change the value only in the SSMS execution window:

```sql
DECLARE @ExecuteScenario bit = 1;
```

Do not commit scenario scripts with execution enabled.

The controlled failure setup script should default to a safe preview mode.

---

## Evidence Captured

The SQL Server setup and validation work was supported by evidence screenshots showing:

| Evidence Area | Description |
|---|---|
| Source database created | `ADF_Ingestion_Source` was created. |
| Source tables created | `dbo` source tables were created. |
| Relationships created | Primary and foreign keys were validated. |
| Sample data inserted | Seed data was inserted successfully. |
| Control metadata created | Control tables and metadata were created. |
| Stored procedures created | ADF control stored procedures were created. |
| Operational validation | SQL scenarios were validated through ADF and control metadata. |

The public evidence index contains the selected implementation evidence for portfolio review.

---

## Design Value

The SQL Server setup demonstrates:

- Local relational source modeling
- Incremental timestamp design
- SQL Server to Azure hybrid ingestion
- Control metadata management
- Stored procedure-driven orchestration
- Evidence-supported validation
- Safe scenario execution practices

This foundation enables Azure Data Factory to process SQL sources in a repeatable and operationally traceable way.