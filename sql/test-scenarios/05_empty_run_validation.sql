/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         05_empty_run_validation.sql
Purpose:        Validate whether SQL source tables have pending incremental
                rows before executing an empty incremental run test.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Empty incremental run validation

IMPORTANT:
- This script is read-only.
- It does not modify source data or control metadata.
- Before the initial full load, this script will show NOT_READY because
  LastWatermarkValue is still the low initial watermark.
- After the initial full load and without additional source changes, this
  script should show zero eligible rows.

Expected empty-run behavior after initial load:
- Eligible_Row_Count = 0
- Validation_Status = PASS_EMPTY_READY
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

/*
===============================================================================
1. Current watermark readiness
===============================================================================
*/

SELECT
    SourceObjectName,
    LastWatermarkValue,
    CASE
        WHEN LastWatermarkValue = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
            THEN 'NOT_READY_INITIAL_LOAD_NOT_COMPLETED'
        ELSE 'READY_FOR_EMPTY_RUN_CHECK'
    END AS Readiness_Status
FROM ctl.SourceObject
WHERE SourceType = 'SQL_TABLE'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
2. Empty run eligibility check
===============================================================================
*/

WITH Watermarks AS
(
    SELECT
        SourceObjectName,
        LastWatermarkValue
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
),
Eligibility AS
(
    SELECT
        'Customers' AS SourceObjectName,
        w.LastWatermarkValue,
        MAX(c.UpdatedAt) AS CurrentHighWatermarkValue,
        SUM
        (
            CASE
                WHEN c.UpdatedAt > w.LastWatermarkValue THEN 1
                ELSE 0
            END
        ) AS Eligible_Row_Count
    FROM dbo.Customers AS c
    CROSS JOIN Watermarks AS w
    WHERE w.SourceObjectName = 'Customers'
    GROUP BY
        w.LastWatermarkValue

    UNION ALL

    SELECT
        'Products',
        w.LastWatermarkValue,
        MAX(p.UpdatedAt),
        SUM
        (
            CASE
                WHEN p.UpdatedAt > w.LastWatermarkValue THEN 1
                ELSE 0
            END
        )
    FROM dbo.Products AS p
    CROSS JOIN Watermarks AS w
    WHERE w.SourceObjectName = 'Products'
    GROUP BY
        w.LastWatermarkValue

    UNION ALL

    SELECT
        'Orders',
        w.LastWatermarkValue,
        MAX(o.UpdatedAt),
        SUM
        (
            CASE
                WHEN o.UpdatedAt > w.LastWatermarkValue THEN 1
                ELSE 0
            END
        )
    FROM dbo.Orders AS o
    CROSS JOIN Watermarks AS w
    WHERE w.SourceObjectName = 'Orders'
    GROUP BY
        w.LastWatermarkValue

    UNION ALL

    SELECT
        'OrderItems',
        w.LastWatermarkValue,
        MAX(oi.UpdatedAt),
        SUM
        (
            CASE
                WHEN oi.UpdatedAt > w.LastWatermarkValue THEN 1
                ELSE 0
            END
        )
    FROM dbo.OrderItems AS oi
    CROSS JOIN Watermarks AS w
    WHERE w.SourceObjectName = 'OrderItems'
    GROUP BY
        w.LastWatermarkValue

    UNION ALL

    SELECT
        'Payments',
        w.LastWatermarkValue,
        MAX(pay.UpdatedAt),
        SUM
        (
            CASE
                WHEN pay.UpdatedAt > w.LastWatermarkValue THEN 1
                ELSE 0
            END
        )
    FROM dbo.Payments AS pay
    CROSS JOIN Watermarks AS w
    WHERE w.SourceObjectName = 'Payments'
    GROUP BY
        w.LastWatermarkValue
)
SELECT
    SourceObjectName,
    LastWatermarkValue,
    CurrentHighWatermarkValue,
    Eligible_Row_Count,
    CASE
        WHEN LastWatermarkValue = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
            THEN 'NOT_READY_INITIAL_LOAD_NOT_COMPLETED'
        WHEN Eligible_Row_Count = 0
            THEN 'PASS_EMPTY_READY'
        ELSE 'CHANGES_PENDING'
    END AS Validation_Status
FROM Eligibility
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
3. Control table run history summary
===============================================================================
*/

SELECT
    SourceObjectName,
    Status,
    COUNT(*) AS Run_Count,
    MAX(StartedAt) AS Last_Run_StartedAt,
    MAX(EndedAt) AS Last_Run_EndedAt
FROM ctl.IngestionRun
GROUP BY
    SourceObjectName,
    Status
ORDER BY
    SourceObjectName,
    Status;
GO

/*
===============================================================================
4. Watermark history summary
===============================================================================
*/

SELECT
    s.SourceObjectName,
    COUNT(wh.WatermarkHistoryId) AS WatermarkHistory_Count,
    MAX(wh.NewWatermarkValue) AS Last_History_Watermark,
    MAX(wh.AppliedAt) AS Last_AppliedAt
FROM ctl.SourceObject AS s
LEFT JOIN ctl.WatermarkHistory AS wh
    ON s.SourceObjectId = wh.SourceObjectId
WHERE s.SourceType = 'SQL_TABLE'
GROUP BY
    s.SourceObjectName
ORDER BY
    s.SourceObjectName;
GO