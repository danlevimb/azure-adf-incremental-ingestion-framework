/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         07_retry_after_failure_validation.sql
Purpose:        Validate retry eligibility after a failed ingestion run.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Retry after failure validation

IMPORTANT:
- This script is read-only.
- It does not modify source data or control metadata.
- Before ADF implementation, this script may show that no failed run exists.
- After a controlled failed run, this script should prove that the watermark
  did not advance and that eligible rows can be retried.

Target:
- dbo.Orders

Expected behavior after controlled failure:
- Latest failed run exists for Orders.
- ctl.SourceObject.LastWatermarkValue remains unchanged.
- ctl.WatermarkHistory has no record for the failed RunId.
- Rows with UpdatedAt greater than LastWatermarkValue remain eligible.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

DECLARE
    @SourceSystemName varchar(100) = 'sales_local',
    @SourceSchema sysname = 'dbo',
    @SourceObjectName sysname = 'Orders',
    @SourceObjectId int,
    @LastWatermarkValue datetime2(3),
    @CurrentHighWatermarkValue datetime2(3),
    @LatestFailedRunId uniqueidentifier;

SELECT
    @SourceObjectId = SourceObjectId,
    @LastWatermarkValue = LastWatermarkValue
FROM ctl.SourceObject
WHERE SourceSystemName = @SourceSystemName
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = @SourceSchema
  AND SourceObjectName = @SourceObjectName;

IF @SourceObjectId IS NULL
BEGIN
    THROW 55001, 'Target SourceObject was not found.', 1;
END;

SELECT
    @CurrentHighWatermarkValue = MAX(UpdatedAt)
FROM dbo.Orders;

SELECT TOP (1)
    @LatestFailedRunId = RunId
FROM ctl.IngestionRun
WHERE SourceObjectId = @SourceObjectId
  AND Status = 'Failed'
ORDER BY
    StartedAt DESC;

/*
===============================================================================
1. Current source object watermark state
===============================================================================
*/

SELECT
    SourceObjectName,
    LastWatermarkValue,
    @CurrentHighWatermarkValue AS CurrentHighWatermarkValue,
    CASE
        WHEN LastWatermarkValue = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
            THEN 'NOT_READY_INITIAL_LOAD_NOT_COMPLETED'
        WHEN @CurrentHighWatermarkValue > LastWatermarkValue
            THEN 'CHANGES_ELIGIBLE_FOR_RETRY'
        ELSE 'NO_CHANGES_PENDING'
    END AS Retry_Readiness_Status
FROM ctl.SourceObject
WHERE SourceObjectId = @SourceObjectId;
GO

/*
===============================================================================
2. Latest failed run for Orders
===============================================================================
*/

DECLARE
    @SourceObjectId_2 int;

SELECT
    @SourceObjectId_2 = SourceObjectId
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

SELECT TOP (1)
    PipelineRunId,
    SourceObjectName,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    NewWatermarkValue,
    RowsRead,
    RowsCopied,
    ErrorMessage,
    StartedAt,
    EndedAt,
    CASE
        WHEN Status = 'Failed'
         AND NewWatermarkValue IS NULL
        THEN 'PASS_FAILED_RUN_RECORDED'
        ELSE 'NO_FAILED_RUN_FOUND_OR_INVALID'
    END AS Validation_Status
FROM ctl.IngestionRun
WHERE SourceObjectId = @SourceObjectId_2
  AND Status = 'Failed'
ORDER BY
    StartedAt DESC;
GO

/*
===============================================================================
3. Watermark history check for latest failed run
Expected:
- Failed runs should not have WatermarkHistory records.
===============================================================================
*/

DECLARE
    @SourceObjectId_3 int,
    @LatestFailedRunId_3 uniqueidentifier;

SELECT
    @SourceObjectId_3 = SourceObjectId
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

SELECT TOP (1)
    @LatestFailedRunId_3 = RunId
FROM ctl.IngestionRun
WHERE SourceObjectId = @SourceObjectId_3
  AND Status = 'Failed'
ORDER BY
    StartedAt DESC;

SELECT
    'Latest failed run watermark history check' AS ValidationArea,
    @LatestFailedRunId_3 AS FailedRunId,
    COUNT(wh.WatermarkHistoryId) AS WatermarkHistory_Count,
    CASE
        WHEN @LatestFailedRunId_3 IS NULL
            THEN 'NO_FAILED_RUN_FOUND_YET'
        WHEN COUNT(wh.WatermarkHistoryId) = 0
            THEN 'PASS_NO_WATERMARK_HISTORY_FOR_FAILED_RUN'
        ELSE 'FAIL_WATERMARK_HISTORY_EXISTS_FOR_FAILED_RUN'
    END AS Validation_Status
FROM ctl.WatermarkHistory AS wh
WHERE wh.RunId = @LatestFailedRunId_3;
GO

/*
===============================================================================
4. Retry eligibility row count for Orders
Expected after controlled failure:
- Eligible_Row_Count should remain greater than 0 if the failed run attempted
  to process pending source changes and the watermark did not advance.
===============================================================================
*/

DECLARE
    @OrdersLastWatermark datetime2(3);

SELECT
    @OrdersLastWatermark = LastWatermarkValue
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

SELECT
    'Orders' AS SourceObjectName,
    @OrdersLastWatermark AS LastWatermarkValue,
    MAX(UpdatedAt) AS CurrentHighWatermarkValue,
    SUM
    (
        CASE
            WHEN UpdatedAt > @OrdersLastWatermark THEN 1
            ELSE 0
        END
    ) AS Eligible_Row_Count,
    CASE
        WHEN @OrdersLastWatermark = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
            THEN 'NOT_READY_INITIAL_LOAD_NOT_COMPLETED'
        WHEN SUM(CASE WHEN UpdatedAt > @OrdersLastWatermark THEN 1 ELSE 0 END) > 0
            THEN 'PASS_ROWS_STILL_ELIGIBLE_FOR_RETRY'
        ELSE 'NO_ROWS_PENDING_FOR_RETRY'
    END AS Validation_Status
FROM dbo.Orders;
GO

/*
===============================================================================
5. Compact retry validation summary
===============================================================================
*/

DECLARE
    @SummarySourceObjectId int,
    @SummaryLatestFailedRunId uniqueidentifier,
    @SummaryLastWatermark datetime2(3),
    @SummaryEligibleRows int,
    @SummaryWatermarkHistoryForFailedRun int;

SELECT
    @SummarySourceObjectId = SourceObjectId,
    @SummaryLastWatermark = LastWatermarkValue
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

SELECT TOP (1)
    @SummaryLatestFailedRunId = RunId
FROM ctl.IngestionRun
WHERE SourceObjectId = @SummarySourceObjectId
  AND Status = 'Failed'
ORDER BY
    StartedAt DESC;

SELECT
    @SummaryEligibleRows = COUNT(*)
FROM dbo.Orders
WHERE UpdatedAt > @SummaryLastWatermark;

SELECT
    @SummaryWatermarkHistoryForFailedRun = COUNT(*)
FROM ctl.WatermarkHistory
WHERE RunId = @SummaryLatestFailedRunId;

SELECT
    'Retry After Failure' AS ValidationArea,
    CASE WHEN @SummaryLatestFailedRunId IS NOT NULL THEN 'YES' ELSE 'NO' END AS Failed_Run_Exists,
    @SummaryEligibleRows AS Eligible_Row_Count,
    ISNULL(@SummaryWatermarkHistoryForFailedRun, 0) AS Failed_Run_WatermarkHistory_Count,
    CASE
        WHEN @SummaryLastWatermark = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
            THEN 'NOT_READY_INITIAL_LOAD_NOT_COMPLETED'
        WHEN @SummaryLatestFailedRunId IS NULL
            THEN 'NO_FAILED_RUN_FOUND_YET'
        WHEN ISNULL(@SummaryWatermarkHistoryForFailedRun, 0) = 0
         AND @SummaryEligibleRows > 0
            THEN 'PASS_RETRY_VALIDATION'
        ELSE 'REVIEW_REQUIRED'
    END AS Validation_Status;
GO