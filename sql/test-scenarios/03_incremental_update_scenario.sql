/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         03_incremental_update_scenario.sql
Purpose:        Generate a controlled incremental update scenario after the
                initial full load has been completed.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Incremental update

IMPORTANT:
- Do NOT execute this scenario in real mode before the initial full load.
- By default, @ExecuteScenario = 0, so the script only previews what it would do.
- To execute the scenario later, set @ExecuteScenario = 1.

Expected business changes:
- Update one existing customer phone
- Update one existing product price
- Update one existing order from CREATED to PAID
- Update one existing payment from PENDING to APPROVED

Expected ADF behavior after execution:
- Customers incremental load captures 1 updated row
- Products incremental load captures 1 updated row
- Orders incremental load captures 1 updated row
- Payments incremental load captures 1 updated row
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

DECLARE @ExecuteScenario bit = 0; -- Change to 1 only when ready to execute after initial full load.

DECLARE
    @ScenarioTimestamp datetime2(3) = SYSUTCDATETIME(),
    @CustomerCode varchar(30) = 'CUST-0001',
    @ProductSku varchar(50) = 'SKU-0001',
    @OrderNumber varchar(50) = 'ORD-00001',
    @NewPhone varchar(30) = '8442990001',
    @NewUnitPrice decimal(18,2) = 49.00,
    @OrderId bigint,
    @ProductId int,
    @NewOrderTotal decimal(18,2);

PRINT 'Incremental update scenario script started.';

IF @ExecuteScenario = 0
BEGIN
    PRINT 'PREVIEW MODE ONLY. No data will be updated.';
    PRINT 'To execute this scenario later, set @ExecuteScenario = 1 after initial full load is complete.';

    SELECT
        'PREVIEW_ONLY' AS Mode,
        @ScenarioTimestamp AS ScenarioTimestamp,
        'This script will update 1 customer, 1 product, 1 order, and 1 payment when @ExecuteScenario = 1.' AS Description;

    SELECT
        SourceObjectName,
        LastWatermarkValue,
        'Rows updated later must have UpdatedAt greater than this value.' AS Note
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
      AND SourceObjectName IN ('Customers', 'Products', 'Orders', 'Payments')
    ORDER BY
        SourceObjectName;

    SELECT
        'Customer before update' AS PreviewArea,
        CustomerCode,
        Phone,
        UpdatedAt
    FROM dbo.Customers
    WHERE CustomerCode = @CustomerCode;

    SELECT
        'Product before update' AS PreviewArea,
        ProductSku,
        UnitPrice,
        UpdatedAt
    FROM dbo.Products
    WHERE ProductSku = @ProductSku;

    SELECT
        'Order and payment before update' AS PreviewArea,
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
      AND SourceObjectName IN ('Customers', 'Products', 'Orders', 'Payments')
      AND LastWatermarkValue > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
)
BEGIN
    THROW 52001, 'Initial full load does not appear to be completed. SQL source watermarks are still at the initial low value.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.Customers
    WHERE CustomerCode = @CustomerCode
)
BEGIN
    THROW 52002, 'Target customer does not exist.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.Products
    WHERE ProductSku = @ProductSku
)
BEGIN
    THROW 52003, 'Target product does not exist.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.Orders
    WHERE OrderNumber = @OrderNumber
      AND OrderStatus = 'CREATED'
)
BEGIN
    THROW 52004, 'Target order does not exist or is not in CREATED status. This scenario should only be executed once unless reset manually.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM dbo.Orders AS o
    INNER JOIN dbo.Payments AS p
        ON o.OrderId = p.OrderId
    WHERE o.OrderNumber = @OrderNumber
      AND p.PaymentStatus = 'PENDING'
)
BEGIN
    THROW 52005, 'Target payment does not exist or is not in PENDING status. This scenario should only be executed once unless reset manually.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM dbo.Customers
    WHERE CustomerCode = @CustomerCode
      AND Phone = @NewPhone
)
BEGIN
    THROW 52006, 'Customer already has the incremental update phone value. Scenario appears to have been executed already.', 1;
END;

/*
===============================================================================
Execute scenario
===============================================================================
*/

BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Updating customer phone...';

    UPDATE dbo.Customers
    SET
        Phone = @NewPhone,
        UpdatedAt = @ScenarioTimestamp
    WHERE CustomerCode = @CustomerCode;

    PRINT 'Updating product price...';

    UPDATE dbo.Products
    SET
        UnitPrice = @NewUnitPrice,
        UpdatedAt = @ScenarioTimestamp
    WHERE ProductSku = @ProductSku;

    SELECT
        @OrderId = OrderId
    FROM dbo.Orders
    WHERE OrderNumber = @OrderNumber;

    SELECT
        @ProductId = ProductId
    FROM dbo.Products
    WHERE ProductSku = @ProductSku;

    /*
        Keep OrderItems unchanged in this scenario.
        Recalculate OrderTotal only if the order contains the updated product.
        This simulates a business correction where the order header reflects
        the latest approved total without changing line-level historical prices.
    */

    SELECT
        @NewOrderTotal = OrderTotal
    FROM dbo.Orders
    WHERE OrderId = @OrderId;

    PRINT 'Updating order status from CREATED to PAID...';

    UPDATE dbo.Orders
    SET
        OrderStatus = 'PAID',
        OrderTotal = @NewOrderTotal,
        UpdatedAt = DATEADD(SECOND, 1, @ScenarioTimestamp)
    WHERE OrderId = @OrderId
      AND OrderStatus = 'CREATED';

    PRINT 'Updating payment status from PENDING to APPROVED...';

    UPDATE dbo.Payments
    SET
        PaymentStatus = 'APPROVED',
        UpdatedAt = DATEADD(SECOND, 2, @ScenarioTimestamp)
    WHERE OrderId = @OrderId
      AND PaymentStatus = 'PENDING';

    COMMIT TRANSACTION;

    PRINT 'Incremental update scenario completed successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'Incremental update scenario failed. Transaction rolled back.';

    THROW;
END CATCH;
GO

/*
===============================================================================
Validation — Incremental update scenario state
===============================================================================
*/

SELECT
    'Customers' AS SourceObjectName,
    COUNT(*) AS Scenario_Row_Count,
    MAX(UpdatedAt) AS Scenario_Max_UpdatedAt
FROM dbo.Customers
WHERE CustomerCode = 'CUST-0001'
  AND Phone = '8442990001'

UNION ALL

SELECT
    'Products',
    COUNT(*),
    MAX(UpdatedAt)
FROM dbo.Products
WHERE ProductSku = 'SKU-0001'
  AND UnitPrice = 49.00

UNION ALL

SELECT
    'Orders',
    COUNT(*),
    MAX(UpdatedAt)
FROM dbo.Orders
WHERE OrderNumber = 'ORD-00001'
  AND OrderStatus = 'PAID'

UNION ALL

SELECT
    'Payments',
    COUNT(*),
    MAX(p.UpdatedAt)
FROM dbo.Payments AS p
INNER JOIN dbo.Orders AS o
    ON p.OrderId = o.OrderId
WHERE o.OrderNumber = 'ORD-00001'
  AND p.PaymentStatus = 'APPROVED'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
Validation — Updated business state
===============================================================================
*/

SELECT
    c.CustomerCode,
    c.Phone,
    pr.ProductSku,
    pr.UnitPrice,
    o.OrderNumber,
    o.OrderStatus,
    p.PaymentStatus,
    c.UpdatedAt AS CustomerUpdatedAt,
    pr.UpdatedAt AS ProductUpdatedAt,
    o.UpdatedAt AS OrderUpdatedAt,
    p.UpdatedAt AS PaymentUpdatedAt
FROM dbo.Orders AS o
INNER JOIN dbo.Customers AS c
    ON o.CustomerId = c.CustomerId
INNER JOIN dbo.Payments AS p
    ON o.OrderId = p.OrderId
CROSS JOIN dbo.Products AS pr
WHERE o.OrderNumber = 'ORD-00001'
  AND pr.ProductSku = 'SKU-0001';
GO