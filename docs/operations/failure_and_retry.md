# Failure and Retry

## Project

`azure-adf-incremental-ingestion-framework`

## Purpose

This document describes the controlled failure and retry validation performed for the ADF Incremental Ingestion Framework.

The goal is to prove that the framework behaves safely when a SQL ingestion run fails.

A professional ingestion framework should not advance the watermark when a copy fails. If the watermark advances after a failed copy, source rows may be skipped permanently in future runs.

This project validates that:

* Failed runs are logged.
* The watermark does not advance after failure.
* No watermark history is written for failed runs.
* Pending source rows remain eligible for retry.
* A later successful retry can process the same pending rows.
* The watermark advances only after successful retry.

---

## Why Failure Handling Matters

Incremental ingestion depends on correct state management.

The stored watermark represents the last successfully processed point in the source system.

If a pipeline fails after detecting new rows but before copying them successfully, the framework must preserve the previous watermark.

Otherwise, the framework could create a data loss scenario.

---

## Core Safety Rule

The main safety rule is:

```text
Never update the stored watermark after a failed ingestion run.
```

In this framework, the stored watermark is:

```text
ctl.SourceObject.LastWatermarkValue
```

Watermark movement history is stored in:

```text
ctl.WatermarkHistory
```

A failed run should not update either of these.

---

## Controlled Failure Scenario

The controlled failure scenario intentionally creates a pending source change and then forces the SQL ingestion pipeline to fail.

This validates the framework under a predictable failure condition.

### Scenario Script

The failure setup script is stored at:

```text
sql/test-scenarios/06_failed_run_setup.sql
```

### Retry Validation Script

The retry readiness validation script is stored at:

```text
sql/test-scenarios/07_retry_after_failure_validation.sql
```

---

## Scenario Overview

The failure and retry validation follows this sequence:

```text
1. Create a pending change in dbo.Orders.
2. Confirm the pending row has UpdatedAt greater than the current Orders watermark.
3. Temporarily alter the Orders destination container to an invalid container.
4. Run PL_01_SQL_Incremental_Ingestion for Orders.
5. Confirm the Copy Activity fails.
6. Confirm the failure path logs the run as Failed.
7. Confirm the watermark does not advance.
8. Confirm no WatermarkHistory record is created for the failed run.
9. Restore the valid destination configuration.
10. Run PL_01_SQL_Incremental_Ingestion for Orders again.
11. Confirm the pending row is copied successfully.
12. Confirm the watermark advances after retry success.
```

---

## 1. Pending Change Creation

Before triggering the failure, a pending change was created in:

```text
dbo.Orders
```

The selected order was:

```text
ORD-00003
```

The source row was updated only by changing `UpdatedAt`.

This created an eligible row without changing business state.

Expected condition:

```text
ORD-00003 UpdatedAt > Orders LastWatermarkValue
```

### Evidence

| Evidence                                         | Description                                                                            |
| ------------------------------------------------ | -------------------------------------------------------------------------------------- |
| `63_failed_run_pending_order_change_created.png` | Shows `ORD-00003` with an `UpdatedAt` value greater than the current Orders watermark. |

---

## 2. Failure Preparation

The controlled failure was created by temporarily changing the destination configuration for `Orders`.

Original expected destination:

```text
DestinationContainer = bronze
```

Temporary invalid destination:

```text
DestinationContainer = bronze_invalid_for_failure_test
```

This causes the SQL Copy Activity to fail because the target container does not exist.

The purpose is not to simulate a random error, but to create a controlled and explainable failure condition.

---

## 3. Failed Pipeline Execution

The SQL incremental ingestion pipeline was executed for `Orders`.

Pipeline:

```text
PL_01_SQL_Incremental_Ingestion
```

Logical run ID:

```text
manual-controlled-failed-run
```

Expected result:

```text
ACT_Copy_SQL_To_ADLS = Failed
ACT_SP_Fail_Ingestion_Run = Succeeded
```

This means the data copy failed, but the failure logging path worked correctly.

---

## 4. Failed Run Expected State

After the failed run, the framework should record the failure without advancing ingestion state.

Expected control table behavior:

| Validation Point                      | Expected Result              |
| ------------------------------------- | ---------------------------- |
| Run status                            | `Failed`                     |
| `NewWatermarkValue`                   | `NULL`                       |
| `ctl.SourceObject.LastWatermarkValue` | Unchanged                    |
| `ctl.WatermarkHistory`                | No record for the failed run |
| Pending source rows                   | Still eligible for retry     |

---

## 5. Retry Readiness Validation

After the failure, retry readiness was validated.

Expected result:

```text
Eligible_Row_Count = 1
Validation_Status  = PASS_ROWS_STILL_ELIGIBLE_FOR_RETRY
```

This confirms that the pending `Orders` row was not skipped.

The row remained eligible because the stored watermark did not move.

### Evidence

| Evidence                                  | Description                                                                            |
| ----------------------------------------- | -------------------------------------------------------------------------------------- |
| `64_controlled_failed_run_validation.png` | Shows failed run logging, no watermark history, and pending row eligibility for retry. |

---

## 6. Configuration Restore

After validating the failed state, the destination configuration was restored.

Restored destination:

```text
DestinationContainer = bronze
```

This allows the retry to write to a valid ADLS Gen2 container.

The failure setup script supports restoring the configuration after the controlled failure test.

---

## 7. Retry Execution

After restoring the valid destination configuration, the SQL incremental ingestion pipeline was executed again for `Orders`.

Pipeline:

```text
PL_01_SQL_Incremental_Ingestion
```

Logical run ID:

```text
manual-retry-after-failure
```

Expected result:

```text
Status = Succeeded
RowsRead = 1
RowsCopied = 1
```

The same pending row was copied successfully.

---

## 8. Retry Success Expected State

After successful retry:

| Validation Point                      | Expected Result                              |
| ------------------------------------- | -------------------------------------------- |
| Run status                            | `Succeeded`                                  |
| `RowsRead`                            | `1`                                          |
| `RowsCopied`                          | `1`                                          |
| `NewWatermarkValue`                   | Updated to pending row `UpdatedAt`           |
| `ctl.SourceObject.LastWatermarkValue` | Advanced                                     |
| `ctl.WatermarkHistory`                | One record inserted for the successful retry |

### Evidence

| Evidence                             | Description                                                                   |
| ------------------------------------ | ----------------------------------------------------------------------------- |
| `65_retry_after_failure_success.png` | Shows successful retry, copied row, updated watermark, and watermark history. |

---

## Failure Path in ADF

The SQL ingestion pipeline includes a failure dependency path from the Copy Activity.

Relevant activity flow:

```text
ACT_Copy_SQL_To_ADLS
    ├── Success → ACT_Lookup_Complete_Ingestion_Run → ACT_SP_Update_Watermark
    └── Failure → ACT_SP_Fail_Ingestion_Run
```

This design ensures that copy success and copy failure are handled differently.

---

## Why the Watermark Is Not Updated on Failure

The watermark update happens only on the success path:

```text
ACT_Copy_SQL_To_ADLS
    → Success
    → ACT_Lookup_Complete_Ingestion_Run
    → ACT_SP_Update_Watermark
```

If `ACT_Copy_SQL_To_ADLS` fails, the pipeline follows the failure path:

```text
ACT_Copy_SQL_To_ADLS
    → Failure
    → ACT_SP_Fail_Ingestion_Run
```

The failure path does not call:

```text
ctl.usp_UpdateWatermark
```

Therefore:

```text
ctl.SourceObject.LastWatermarkValue
```

does not change.

---

## Control Metadata Used

The failure and retry validation uses the following control tables:

| Table                  | Purpose                                                     |
| ---------------------- | ----------------------------------------------------------- |
| `ctl.SourceObject`     | Stores the current watermark and destination configuration. |
| `ctl.IngestionRun`     | Stores failed and successful run records.                   |
| `ctl.WatermarkHistory` | Proves that only successful runs advance watermarks.        |

---

## Key Validation Queries

Typical validation checks include:

```sql
SELECT
    PipelineRunId,
    SourceObjectName,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    NewWatermarkValue,
    RowsRead,
    RowsCopied,
    DestinationContainer,
    DestinationFolder,
    ErrorMessage
FROM ctl.IngestionRun
WHERE PipelineRunId IN
(
    'manual-controlled-failed-run',
    'manual-retry-after-failure'
);
```

```sql
SELECT
    SourceObjectName,
    LastWatermarkValue
FROM ctl.SourceObject
WHERE SourceObjectName = 'Orders';
```

```sql
SELECT
    s.SourceObjectName,
    wh.PreviousWatermarkValue,
    wh.NewWatermarkValue,
    wh.AppliedByPipelineRunId,
    wh.AppliedAt
FROM ctl.WatermarkHistory AS wh
INNER JOIN ctl.SourceObject AS s
    ON wh.SourceObjectId = s.SourceObjectId
WHERE s.SourceObjectName = 'Orders';
```

---

## Validated Behavior Summary

| Scenario Step                                    | Result |
| ------------------------------------------------ | ------ |
| Pending Orders change created                    | Passed |
| Controlled destination failure prepared          | Passed |
| SQL Copy Activity failed as expected             | Passed |
| Failed run logged                                | Passed |
| Watermark did not advance                        | Passed |
| WatermarkHistory not inserted for failed run     | Passed |
| Pending row remained eligible                    | Passed |
| Destination configuration restored               | Passed |
| Retry succeeded                                  | Passed |
| Watermark advanced after retry                   | Passed |
| WatermarkHistory inserted after successful retry | Passed |

---

## Final Safety Check

The final operational validation summary confirmed:

```text
PASS_FAILED_RUN_DID_NOT_ADVANCE_WATERMARK
```

This is one of the most important validation results in the project.

It proves that the framework protects incremental state during failures.

### Evidence

| Evidence                                      | Description                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------- |
| `66_operational_validation_final_summary.png` | Final summary showing controlled failed run and retry validations passed. |

---

## Design Value

Failure and retry validation demonstrates that the framework is not only a happy-path ingestion demo.

It shows operational thinking:

* Failed runs are expected.
* Failed runs are logged.
* Watermarks are protected.
* Data is not skipped after failure.
* Retry behavior is testable.
* Control metadata provides auditability.

This makes the project stronger as a professional Azure Data Engineering portfolio artifact.

---

## Future Improvements

Potential future enhancements:

* Add automatic retry policies at the ADF activity level.
* Add alerting when a run fails.
* Add monitoring dashboards for failed runs.
* Add retry queue logic.
* Add detailed step-level logging in `ctl.IngestionRunStep`.
* Add notification integration through email, Teams, or Azure Monitor.
* Add failure categories for better troubleshooting.
* Add configurable maximum retry attempts.

These enhancements were intentionally deferred to keep the MVP focused and cost-aware.