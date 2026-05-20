# Watermark Strategy

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the watermark strategy used by the SQL incremental ingestion pipeline.

The framework uses a datetime-based high watermark pattern to identify new or changed rows in SQL Server source tables and copy only eligible records into Azure Data Lake Storage Gen2.

The goal is to support repeatable incremental ingestion while preventing data loss when failures occur.

---

## Strategy Summary

![Control Metadata Design](../../diagrams/03_control_metadata_watermark_flow.png)

The approved MVP strategy is:

```text
Datetime-based high watermark
```

The watermark column used by the SQL source tables is:

```text
UpdatedAt
```

Each SQL source table maintains its own independent watermark in:

```text
ctl.SourceObject.LastWatermarkValue
```

The SQL source tables included in the MVP are:

```text
dbo.Customers
dbo.Products
dbo.Orders
dbo.OrderItems
dbo.Payments
```

---

## Why `UpdatedAt` Was Used

The `UpdatedAt` column was selected because it supports both:

- Insert detection
- Update detection

When a new row is inserted, `UpdatedAt` is populated.

When an existing row changes, `UpdatedAt` is updated.

This allows the framework to detect both new and changed records using the same incremental extraction pattern.

---

## Initial Low Watermark

Each SQL source object starts with the initial low watermark:

```text
1900-01-01 00:00:00.000
```

This allows the first execution to behave like an initial full load while still using the incremental extraction logic.

Example:

```sql
WHERE UpdatedAt > '1900-01-01 00:00:00.000'
  AND UpdatedAt <= @CurrentHighWatermarkValue
```

After a successful run, the stored watermark advances to the high watermark captured for that run.

---

## Extraction Window

The framework uses a closed upper-bound extraction window.

```sql
WHERE UpdatedAt > @LastWatermarkValue
  AND UpdatedAt <= @CurrentHighWatermarkValue
```

This pattern means:

| Condition | Meaning |
|---|---|
| `UpdatedAt > @LastWatermarkValue` | Exclude rows already processed by previous successful runs. |
| `UpdatedAt <= @CurrentHighWatermarkValue` | Include rows up to the high watermark captured before the copy began. |

---

## Why Capture Current High Watermark Before Copy

The pipeline captures the current high watermark before running the copy activity.

This prevents the copy from chasing rows that continue changing while the pipeline is running.

The flow is:

```text
1. Read previous watermark from ctl.SourceObject.
2. Query the source table for MAX(UpdatedAt).
3. Store that value as CurrentHighWatermarkValue for the run.
4. Copy rows within the extraction window.
5. Update LastWatermarkValue only after successful copy.
```

---

## Core Watermark Rule

The most important rule is:

```text
The stored watermark is updated only after a successful copy.
```

This prevents data loss.

If a pipeline fails after detecting eligible rows but before copying them successfully, the framework must not advance the watermark.

---

## Success Behavior

When a SQL ingestion run succeeds:

1. The run is marked as `Succeeded` in `ctl.IngestionRun`.
2. `RowsRead` and `RowsCopied` are recorded when available.
3. `NewWatermarkValue` is stored in `ctl.IngestionRun`.
4. `ctl.SourceObject.LastWatermarkValue` is updated.
5. A row is inserted into `ctl.WatermarkHistory`.

Example:

```text
OldWatermarkValue          = 2026-05-15 16:15:41.222
CurrentHighWatermarkValue  = 2026-05-15 16:41:20.628
NewWatermarkValue          = 2026-05-15 16:41:20.628
Status                     = Succeeded
RowsCopied                 = 1
```

---

## Failure Behavior

When a SQL ingestion run fails:

1. The run is marked as `Failed` in `ctl.IngestionRun`.
2. The error message is stored.
3. `NewWatermarkValue` remains `NULL`.
4. `ctl.SourceObject.LastWatermarkValue` is not updated.
5. No row is inserted into `ctl.WatermarkHistory`.
6. The source rows remain eligible for retry.

Expected failed-run state:

| Field | Expected Value |
|---|---|
| `Status` | `Failed` |
| `NewWatermarkValue` | `NULL` |
| `WatermarkHistory_Count` | `0` |
| Eligible rows after failure | Greater than `0` when pending rows exist |

---

## Retry Behavior

Because failed runs do not advance the watermark, retry remains safe.

Retry flow:

```text
1. A pending row exists after the previous watermark.
2. A controlled failure occurs.
3. The failed run is logged.
4. The watermark remains unchanged.
5. The destination issue is corrected.
6. The pipeline is executed again.
7. The same pending row is copied successfully.
8. The watermark advances after success.
9. Watermark history is inserted for the successful retry.
```

This was validated with the retry after failure scenario.

---

## Independent Watermarks Per Source Object

Each SQL source object has its own watermark.

This means one table can advance without affecting the others.

Example:

| SourceObjectName | WatermarkColumn | LastWatermarkValue |
|---|---|---|
| `Customers` | `UpdatedAt` | Independent timestamp |
| `Products` | `UpdatedAt` | Independent timestamp |
| `Orders` | `UpdatedAt` | Independent timestamp |
| `OrderItems` | `UpdatedAt` | Independent timestamp |
| `Payments` | `UpdatedAt` | Independent timestamp |

This supports realistic source behavior because different tables may receive changes at different times.

---

## Stored Procedures Involved

The watermark strategy is implemented through the control metadata stored procedure interface.

| Stored Procedure | Role |
|---|---|
| `ctl.usp_GetSourceObjectConfig` | Reads the previous watermark and source configuration. |
| `ctl.usp_GetCurrentHighWatermark` | Captures the current `MAX(UpdatedAt)` for the source table. |
| `ctl.usp_StartIngestionRun` | Logs run start and stores the old/current watermark values. |
| `ctl.usp_CompleteIngestionRun` | Marks the run as successful and records copy metrics. |
| `ctl.usp_FailIngestionRun` | Marks the run as failed and stores the error message. |
| `ctl.usp_UpdateWatermark` | Updates the stored watermark and writes watermark history. |

---

## ADF Pipeline Activities Involved

The SQL incremental pipeline uses the following activities for watermark handling:

| Activity | Purpose |
|---|---|
| `ACT_Lookup_Source_Config` | Reads source configuration and last watermark. |
| `ACT_Lookup_Current_High_Watermark` | Captures current high watermark. |
| `ACT_Lookup_Start_Ingestion_Run` | Creates a started run record. |
| `ACT_Copy_SQL_To_ADLS` | Copies rows within the watermark window. |
| `ACT_Lookup_Complete_Ingestion_Run` | Completes the run after successful copy. |
| `ACT_SP_Update_Watermark` | Updates stored watermark after success. |
| `ACT_SP_Fail_Ingestion_Run` | Logs failure if copy fails. |

---

## Example Incremental Insert

A new order is inserted with:

```text
UpdatedAt = 2026-05-14 19:53:21.703
```

Previous watermark:

```text
LastWatermarkValue = 2026-05-02 20:35:00.000
```

The row is eligible because:

```text
2026-05-14 19:53:21.703 > 2026-05-02 20:35:00.000
```

After successful ingestion:

```text
LastWatermarkValue = 2026-05-14 19:53:21.703
```

---

## Example Incremental Update

An existing order is updated from one business state to another.

Example:

```text
OrderStatus = PAID
UpdatedAt   = 2026-05-14 21:22:32.144
```

The framework captures the update because the `UpdatedAt` value is greater than the stored watermark.

---

## Example Refund Scenario

A refund scenario updates both:

```text
dbo.Orders
dbo.Payments
```

Expected business transition:

```text
Orders.OrderStatus     = REFUNDED
Payments.PaymentStatus = REFUNDED
```

Each affected table advances independently after successful ingestion.

---

## Empty Run Behavior

An empty run validation checks whether there are any rows eligible for ingestion.

Expected empty state:

```text
Eligible_Row_Count = 0
Validation_Status  = PASS_EMPTY_READY
```

This means:

```text
MAX(UpdatedAt) <= LastWatermarkValue
```

The empty run validation confirms that the framework has already processed all eligible source changes.

---

## Controlled Failure Validation

The controlled failure scenario intentionally changes the destination configuration for `Orders` so that the copy fails.

Expected validation results:

```text
Status = Failed
NewWatermarkValue = NULL
WatermarkHistory_Count = 0
Eligible_Row_Count = 1
Validation_Status = PASS_ROWS_STILL_ELIGIBLE_FOR_RETRY
```

This proves that failed runs do not advance the watermark and do not lose pending changes.

---

## Final Operational Validation

The final operational validation confirmed:

| Scenario | Result |
|---|---|
| Incremental Insert | PASS |
| Incremental Update | PASS |
| Refund Scenario | PASS |
| Controlled Failed Run | PASS |
| Retry After Failure | PASS |

The failed-run safety check also confirmed:

```text
PASS_FAILED_RUN_DID_NOT_ADVANCE_WATERMARK
```

---

## Limitations

This MVP uses a simple datetime watermark strategy.

Known limitations:

- It assumes `UpdatedAt` is reliably maintained by the source system.
- It does not handle deletes.
- It does not use SQL Server CDC or Change Tracking.
- It does not use a composite watermark.
- It does not resolve ties when multiple rows share the same `UpdatedAt` at sub-millisecond precision.
- It does not implement late-arriving correction windows.

These limitations are acceptable for the MVP and should be documented as future improvement opportunities.

---

## Future Improvements

Potential future enhancements:

- Add composite watermark support using `UpdatedAt` plus a surrogate key.
- Add SQL Server Change Tracking or CDC.
- Add soft-delete handling.
- Add configurable lookback windows.
- Add validation for duplicate or out-of-order updates.
- Move control metadata to Azure SQL Database for a more cloud-native version.
- Add automated monitoring dashboards.
- Add alerting for failed ingestion runs.

---

## Design Value

The watermark strategy demonstrates:

- Incremental loading
- Failure-safe state management
- Independent source tracking
- Retry readiness
- Auditable watermark movement
- Operational validation through control metadata
- Practical Azure Data Engineering orchestration patterns

This strategy keeps the MVP understandable while still showing a professional ingestion framework design.