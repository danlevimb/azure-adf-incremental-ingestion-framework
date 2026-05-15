/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         04_refund_scenario.sql
Purpose:        Generate a controlled refund scenario after the initial full
                load has been completed.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Refund scenario

IMPORTANT:
- Do NOT execute this scenario in real mode before the initial full load.
- By default, @ExecuteScenario = 0, so the script only previews what it would do.
- To execute the scenario later, set @ExecuteScenario = 1.

Expected business changes:
- Update one existing paid order to REFUNDED
- Update the related approved payment to REFUNDED

Expected ADF behavior after execution:
- Orders incremental load captures 1 updated row
- Payments incremental load captures 1 updated row
- Orders and Payments advance their own watermarks independently

Business meaning:
- A single refund event can create incremental changes in multiple related tables.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

DECLARE @ExecuteScenario bit = 0; -- Change to 1 only when ready to execute after initial full load.

DECLARE
    @ScenarioTimestamp datetime2(3) = SYSUTCDATETIME(),
    @OrderNumber varchar(50) = 'ORD-00002',
    @OrderId bigint;

PRINT 'Refund scenario script started.';

IF @ExecuteScenario = 0
BEGIN
    PRINT 'PREVIEW MODE ONLY. No data will be updated.';
    PRINT 'To execute this scenario later, set @ExecuteScenario = 1 after initial full load is complete.';

    SELECT
        'PREVIEW_ONLY' AS Mode,
        @ScenarioTimestamp AS ScenarioTimestamp,
        'This script will update 1 paid order and 1 approved payment to REFUNDED when @ExecuteScenario = 1.' AS Description;

    SELECT
        SourceObjectName,
        LastWatermarkValue,
        'Rows updated later must have UpdatedAt greater than this value.' AS Note
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
      AND SourceObjectName IN ('Orders', 'Payments')
    ORDER BY
        SourceObjectName;

    SELECT
        'Refund target before update' AS PreviewArea,
        o.OrderNumber,
        o.OrderStatus,
        o.OrderTotal,
        p.PaymentStatus,
        p.PaymentAmount,
        o.UpdatedAt AS OrderUpdatedAt,
        p.UpdatedAt AS PaymentUpdatedAt
    FROM dbo.Orders AS o
    INNER JOIN dbo.Payments AS p
        ON o.OrderId = p.OrderId
    WHERE o.OrderNumber = @OrderNumber;

    SELECT
        'Refund eligible paid orders' AS PreviewArea,
        TOP_Orders.OrderNumber,
        TOP_Orders.OrderStatus,
        TOP_Orders.PaymentStatus,
        TOP_Orders.OrderUpdatedAt,
        TOP_Orders.PaymentUpdatedAt
    FROM
    (
        SELECT TOP (10)
            o.OrderNumber,
            o.OrderStatus,
            p.PaymentStatus,
            o.UpdatedAt AS OrderUpdatedAt,
            p.UpdatedAt AS PaymentUpdatedAt
        FROM dbo.Orders AS o
        INNER JOIN dbo.Payments AS p
            ON o.OrderId = p.OrderId
        WHERE o.OrderStatus IN ('PAID', 'COMPLETED')
          AND p.PaymentStatus = 'APPROVED'
        ORDER BY
            o.OrderId
    ) AS TOP_Orders;

    RETURN;
END;

/*
===============================================================================
Safety checks
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
      AND SourceObjectName IN ('Orders', 'Payments')
      AND LastWatermarkValue > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
)
BEGIN
    THROW 53001, 'Initial full load does not appear to be completed. SQL source watermarks are still at the initial low value.', 1;
END;

SELECT
    @OrderId = o.OrderId
FROM dbo.Orders AS o
WHERE o.OrderNumber = @OrderNumber;

IF @OrderId IS NULL
BEGIN
    THROW 53002, 'Target order does not exist.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.Orders AS o
    INNER JOIN dbo.Payments AS p
        ON o.OrderId = p.OrderId
    WHERE o.OrderId = @OrderId
      AND o.OrderStatus IN ('PAID', 'COMPLETED')
      AND p.PaymentStatus = 'APPROVED'
)
BEGIN
    THROW 53003, 'Target order/payment is not eligible for refund. Expected OrderStatus PAID or COMPLETED and PaymentStatus APPROVED.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM dbo.Orders AS o
    INNER JOIN dbo.Payments AS p
        ON o.OrderId = p.OrderId
    WHERE o.OrderId = @OrderId
      AND (o.OrderStatus = 'REFUNDED' OR p.PaymentStatus = 'REFUNDED')
)
BEGIN
    THROW 53004, 'Refund scenario appears to have already been executed for this order.', 1;
END;

/*
===============================================================================
Execute scenario
===============================================================================
*/

BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Updating order status to REFUNDED...';

    UPDATE dbo.Orders
    SET
        OrderStatus = 'REFUNDED',
        UpdatedAt = @ScenarioTimestamp
    WHERE OrderId = @OrderId
      AND OrderStatus IN ('PAID', 'COMPLETED');

    PRINT 'Updating payment status to REFUNDED...';

    UPDATE dbo.Payments
    SET
        PaymentStatus = 'REFUNDED',
        UpdatedAt = DATEADD(SECOND, 1, @ScenarioTimestamp)
    WHERE OrderId = @OrderId
      AND PaymentStatus = 'APPROVED';

    COMMIT TRANSACTION;

    PRINT 'Refund scenario completed successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'Refund scenario failed. Transaction rolled back.';

    THROW;
END CATCH;
GO

/*
===============================================================================
Validation — Refund scenario state
===============================================================================
*/

SELECT
    'Orders' AS SourceObjectName,
    COUNT(*) AS Scenario_Row_Count,
    MAX(UpdatedAt) AS Scenario_Max_UpdatedAt
FROM dbo.Orders
WHERE OrderNumber = 'ORD-00002'
  AND OrderStatus = 'REFUNDED'

UNION ALL

SELECT
    'Payments',
    COUNT(*),
    MAX(p.UpdatedAt)
FROM dbo.Payments AS p
INNER JOIN dbo.Orders AS o
    ON p.OrderId = o.OrderId
WHERE o.OrderNumber = 'ORD-00002'
  AND p.PaymentStatus = 'REFUNDED'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
Validation — Refunded business state
===============================================================================
*/

SELECT
    o.OrderNumber,
    o.OrderStatus,
    p.PaymentStatus,
    o.OrderTotal,
    p.PaymentAmount,
    o.UpdatedAt AS OrderUpdatedAt,
    p.UpdatedAt AS PaymentUpdatedAt
FROM dbo.Orders AS o
INNER JOIN dbo.Payments AS p
    ON o.OrderId = p.OrderId
WHERE o.OrderNumber = 'ORD-00002';
GO