# Dynamic Datasets

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the dynamic datasets used by the ADF Incremental Ingestion Framework.

Dynamic datasets allow Azure Data Factory to process multiple SQL tables, CSV files, JSON files, and ADLS output paths without creating a separate dataset for every source object.

The framework uses parameterized datasets to support:

- Metadata-driven SQL table access
- Dynamic ADLS Parquet output paths
- Dynamic CSV file ingestion
- Dynamic JSON file ingestion
- Reusable pipeline design
- Cleaner source-to-target routing

---

## Dataset Inventory

The project uses four main dynamic datasets:

| Dataset | Purpose |
|---|---|
| `DS_SQLSERVER_TABLE_DYNAMIC` | Reads SQL Server tables dynamically using schema and table parameters. |
| `DS_ADLS_PARQUET_DYNAMIC` | Writes SQL extraction outputs to ADLS Gen2 using dynamic container, folder, and file parameters. |
| `DS_ADLS_CSV_DYNAMIC` | Reads and writes CSV files using dynamic ADLS paths. |
| `DS_ADLS_JSON_DYNAMIC` | Reads and writes JSON files using dynamic ADLS paths. |

---

## Why Dynamic Datasets Were Used

Without dynamic datasets, the project would need separate datasets for each source object.

For example:

```text
Customers dataset
Products dataset
Orders dataset
OrderItems dataset
Payments dataset
currency_rates CSV dataset
country_currency CSV dataset
source_system_metadata JSON dataset
manual_adjustments JSON dataset
```

That approach does not scale well.

Instead, this project uses one reusable dataset per source/format pattern and passes runtime parameters from the pipelines.

This supports a metadata-driven ingestion framework.

---

## 1. `DS_SQLSERVER_TABLE_DYNAMIC`

### Purpose

`DS_SQLSERVER_TABLE_DYNAMIC` is used to access SQL Server tables dynamically.

It supports both:

- Metadata lookups
- SQL source extraction

### Linked Service

```text
LS_SQLSERVER_LOCAL_SHIR
```

or, when used for control metadata queries:

```text
LS_CONTROL_SQLSERVER_LOCAL
```

depending on the activity configuration.

### Dataset Parameters

| Parameter | Purpose |
|---|---|
| `schemaName` | SQL schema name, such as `dbo` or `ctl`. |
| `tableName` | SQL table name, such as `Orders` or `SourceObject`. |

### Example Values

```text
schemaName = dbo
tableName  = Orders
```

or:

```text
schemaName = ctl
tableName  = SourceObject
```

### Dynamic Object Pattern

The dataset can resolve SQL objects dynamically using:

```text
@dataset().schemaName
@dataset().tableName
```

This allows the same dataset to point to different tables at runtime.

---

## Usage in SQL Ingestion

In `PL_01_SQL_Incremental_Ingestion`, this dataset is used to support:

- Source configuration lookup
- Current high watermark lookup
- Start ingestion run lookup
- Incremental SQL copy source
- Complete ingestion run lookup

The actual extraction query is built dynamically using metadata from `ctl.SourceObject`.

---

## Example Runtime Behavior

When processing `Orders`, ADF passes:

```text
schemaName = dbo
tableName  = Orders
```

The pipeline then reads from:

```text
dbo.Orders
```

When reading control metadata, ADF may pass:

```text
schemaName = ctl
tableName  = SourceObject
```

The pipeline then reads metadata from:

```text
ctl.SourceObject
```

---

## 2. `DS_ADLS_PARQUET_DYNAMIC`

### Purpose

`DS_ADLS_PARQUET_DYNAMIC` is used as the ADLS Gen2 sink for SQL Server extraction outputs.

SQL Server data is written to ADLS Gen2 in Parquet format.

### Linked Service

```text
LS_ADLSGEN2_DEV
```

### Dataset Parameters

| Parameter | Purpose |
|---|---|
| `containerName` | ADLS Gen2 container, such as `bronze`. |
| `folderPath` | Dynamic destination folder path. |
| `fileName` | Output file name, such as `orders.parquet`. |

### Example Values

```text
containerName = bronze
folderPath    = sqlserver/sales_local/dbo/orders/load_date=2026-05-15/run_id=<run_id>
fileName      = orders.parquet
```

### Output Pattern

```text
bronze/sqlserver/<source_system>/<schema>/<table>/load_date=YYYY-MM-DD/run_id=<run_id>/<table>.parquet
```

### Why Parquet Was Used

Parquet was selected for SQL extraction outputs because it is a common analytical file format for lake-based data engineering.

It provides a better foundation for future downstream processing than plain CSV.

---

## Usage in SQL Copy Activity

`DS_ADLS_PARQUET_DYNAMIC` is used by:

```text
ACT_Copy_SQL_To_ADLS
```

as the sink dataset.

The destination path is constructed dynamically from:

- Source system name
- Schema name
- Source object name
- Load date
- Run ID

This makes each pipeline execution traceable.

---

## 3. `DS_ADLS_CSV_DYNAMIC`

### Purpose

`DS_ADLS_CSV_DYNAMIC` is used for CSV file ingestion.

It supports reading CSV files from ADLS `landing` and writing them into ADLS `bronze`.

### Linked Service

```text
LS_ADLSGEN2_DEV
```

### Dataset Parameters

| Parameter | Purpose |
|---|---|
| `containerName` | ADLS container name. |
| `folderPath` | Folder path where the CSV file is located or written. |
| `fileName` | CSV file name. |

### CSV Settings

The dataset was configured with:

| Setting | Value |
|---|---|
| Column delimiter | Comma `,` |
| Row delimiter | Default |
| Encoding | UTF-8 |
| Quote character | Double quote `"` |
| Escape character | Backslash `\` |
| First row as header | Enabled |

### Example Source Path

```text
landing/files/csv/currency_rates/currency_rates.csv
```

### Example Bronze Path

```text
bronze/files/csv/currency_rates/load_date=2026-05-15/run_id=<run_id>/currency_rates.csv
```

---

## Usage in File Ingestion Pipeline

`DS_ADLS_CSV_DYNAMIC` is used by:

```text
ACT_Copy_CSV_To_Bronze
```

inside:

```text
PL_02_File_Ingestion
```

The pipeline reads file configuration from control metadata and passes runtime parameters into the dataset.

---

## 4. `DS_ADLS_JSON_DYNAMIC`

### Purpose

`DS_ADLS_JSON_DYNAMIC` is used for JSON file ingestion.

It supports reading JSON files from ADLS `landing` and writing them into ADLS `bronze`.

### Linked Service

```text
LS_ADLSGEN2_DEV
```

### Dataset Parameters

| Parameter | Purpose |
|---|---|
| `containerName` | ADLS container name. |
| `folderPath` | Folder path where the JSON file is located or written. |
| `fileName` | JSON file name. |

### JSON Settings

The dataset was configured for UTF-8 encoded JSON files.

### Example Source Path

```text
landing/files/json/source_system_metadata/source_system_metadata.json
```

### Example Bronze Path

```text
bronze/files/json/source_system_metadata/load_date=2026-05-15/run_id=<run_id>/source_system_metadata.json
```

---

## Usage in File Ingestion Pipeline

`DS_ADLS_JSON_DYNAMIC` is used by:

```text
ACT_Copy_JSON_To_Bronze
```

inside:

```text
PL_02_File_Ingestion
```

The pipeline uses `SourceType` to decide whether to process the source object as CSV or JSON.

---

## Runtime Parameter Resolution

ADF resolves dataset parameters at runtime.

For example, when `PL_02_File_Ingestion` processes `source_system_metadata`, the JSON dataset receives values similar to:

```text
containerName = landing
folderPath    = files/json/source_system_metadata
fileName      = source_system_metadata.json
```

For the bronze destination, it receives values similar to:

```text
containerName = bronze
folderPath    = files/json/source_system_metadata/load_date=2026-05-15/run_id=<run_id>
fileName      = source_system_metadata.json
```

---

## Dynamic Dataset Validation

Each dynamic dataset was tested during implementation.

Evidence includes:

| Evidence | Description |
|---|---|
| `43_dynamic_sqlserver_dataset.png` | SQL Server dynamic dataset preview validated. |
| `44_dynamic_adls_parquet_dataset.png` | ADLS Parquet dataset configured for dynamic output paths. |
| `45_dynamic_adls_csv_dataset.png` | CSV dynamic dataset preview validated. |
| `46_dynamic_adls_json_dataset.png` | JSON dynamic dataset preview validated. |

---

## Important ADF Behavior

When validating dynamic datasets in Azure Data Factory Studio, preview can fail if the dataset parameters point to a path or file that does not exist yet.

This is expected behavior.

For example, a dynamic sink path may not exist until a pipeline execution creates it.

Therefore, dataset validation should be interpreted carefully:

| Situation | Interpretation |
|---|---|
| Linked service connection succeeds | Connectivity is valid. |
| Dataset preview succeeds with real parameters | Dataset path and format are valid. |
| Dataset preview fails with placeholder paths | Not necessarily a configuration error. |
| Pipeline execution succeeds | Runtime parameter resolution is valid. |

---

## Dynamic Dataset Benefits

Dynamic datasets provide several benefits:

| Benefit | Explanation |
|---|---|
| Reusability | One dataset can process many source objects. |
| Maintainability | New sources can be added through metadata rather than creating new datasets. |
| Cleaner ADF design | Fewer duplicated datasets and activities. |
| Metadata-driven ingestion | Pipelines can use control table values at runtime. |
| Source-to-target traceability | Paths can include source system, object name, load date, and run ID. |
| Portfolio value | Demonstrates scalable ADF design patterns. |

---

## Relationship to Control Metadata

Dynamic datasets depend on metadata stored in SQL Server control tables.

Important metadata values include:

| Metadata Value | Used For |
|---|---|
| `SourceSchema` | SQL Server schema parameter. |
| `SourceObjectName` | SQL table name or file object name. |
| `SourceType` | Determines SQL vs CSV vs JSON routing. |
| `DestinationContainer` | ADLS target container. |
| `DestinationFolder` | ADLS target folder path. |
| `DestinationFormat` | Target output format. |
| `FileNamePattern` | File name resolution for file-based ingestion. |
| `SourcePath` | Landing path for file-based ingestion. |

---

## Design Value

The dynamic dataset strategy demonstrates:

- Reusable ADF dataset design
- Runtime parameterization
- Metadata-driven ingestion
- Dynamic ADLS path construction
- SQL table abstraction
- CSV and JSON file abstraction
- Cleaner pipeline implementation
- Better scalability for additional sources

This approach is more realistic than creating one static dataset per table or file.