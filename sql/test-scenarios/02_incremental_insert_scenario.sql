/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         02_incremental_insert_scenario.sql
Purpose:        Generate a controlled incremental insert scenario after the
                initial full load has been completed.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Incremental insert

IMPORTANT:
- Do NOT execute this scenario in real mode before the initial full load.
- By default, @ExecuteScenario = 0, so the script only previews what it would do.
- To execute the scenario later, set @ExecuteScenario = 1.

Expected business changes:
- Insert one new customer
- Insert one new order
- Insert two order items
- Insert one pending payment

Expected ADF behavior after execution:
- Customers incremental load captures 1 row
- Orders incremental load captures 1 row
- OrderItems incremental load captures 2 rows
- Payments incremental load captures 1 row
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

DECLARE @ExecuteScenario bit = 0; -- Change to 1 only when ready to execute after initial full load.

DECLARE
    @ScenarioTimestamp datetime2(3) = SYSUTCDATETIME(),
    @CustomerId int,
    @OrderId bigint,
    @ProductId1 int,
    @ProductId2 int,
    @UnitPrice1 decimal(18,2),
    @UnitPrice2 decimal(18,2),
    @Quantity1 int = 2,
    @Quantity2 int = 1,
    @OrderTotal decimal(18,2);

PRINT 'Incremental insert scenario script started.';

IF @ExecuteScenario = 0
BEGIN
    PRINT 'PREVIEW MODE ONLY. No data will be inserted.';
    PRINT 'To execute this scenario later, set @ExecuteScenario = 1 after initial full load is complete.';

    SELECT
        'PREVIEW_ONLY' AS Mode,
        @ScenarioTimestamp AS ScenarioTimestamp,
        'This script will insert 1 customer, 1 order, 2 order items, and 1 payment when @ExecuteScenario = 1.' AS Description;

    SELECT
        SourceObjectName,
        LastWatermarkValue,
        'Rows inserted later must have UpdatedAt greater than this value.' AS Note
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
      AND SourceObjectName IN ('Customers', 'Orders', 'OrderItems', 'Payments')
    ORDER BY
        SourceObjectName;

    RETURN;
END;

/*
===============================================================================
Safety checks
===============================================================================
*/

IF EXISTS
(
    SELECT 1
    FROM dbo.Customers
    WHERE CustomerCode = 'CUST-INC-0001'
)
BEGIN
    THROW 51001, 'Incremental insert scenario customer already exists. This scenario should only be executed once unless reset manually.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM dbo.Orders
    WHERE OrderNumber = 'ORD-INC-0001'
)
BEGIN
    THROW 51002, 'Incremental insert scenario order already exists. This scenario should only be executed once unless reset manually.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceType = 'SQL_TABLE'
      AND SourceObjectName IN ('Customers', 'Orders', 'OrderItems', 'Payments')
      AND LastWatermarkValue > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
)
BEGIN
    THROW 51003, 'Initial full load does not appear to be completed. SQL source watermarks are still at the initial low value.', 1;
END;

/*
===============================================================================
Execute scenario
===============================================================================
*/

BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Inserting incremental customer...';

    INSERT INTO dbo.Customers
    (
        CustomerCode,
        FirstName,
        LastName,
        Email,
        Phone,
        City,
        StateName,
        CountryCode,
        IsActive,
        CreatedAt,
        UpdatedAt
    )
    VALUES
    (
        'CUST-INC-0001',
        'Incremental',
        'Customer',
        'incremental.customer@example.com',
        '8442000001',
        'Saltillo',
        'Coahuila',
        'MX',
        1,
        @ScenarioTimestamp,
        @ScenarioTimestamp
    );

    SET @CustomerId = SCOPE_IDENTITY();

    SELECT TOP (1)
        @ProductId1 = ProductId,
        @UnitPrice1 = UnitPrice
    FROM dbo.Products
    WHERE ProductSku = 'SKU-0001';

    SELECT TOP (1)
        @ProductId2 = ProductId,
        @UnitPrice2 = UnitPrice
    FROM dbo.Products
    WHERE ProductSku = 'SKU-0003';

    SET @OrderTotal =
        (@Quantity1 * @UnitPrice1)
        + (@Quantity2 * @UnitPrice2);

    PRINT 'Inserting incremental order...';

    INSERT INTO dbo.Orders
    (
        OrderNumber,
        CustomerId,
        OrderDate,
        OrderStatus,
        OrderTotal,
        CurrencyCode,
        SalesChannel,
        CreatedAt,
        UpdatedAt
    )
    VALUES
    (
        'ORD-INC-0001',
        @CustomerId,
        @ScenarioTimestamp,
        'CREATED',
        @OrderTotal,
        'MXN',
        'ONLINE',
        @ScenarioTimestamp,
        @ScenarioTimestamp
    );

    SET @OrderId = SCOPE_IDENTITY();

    PRINT 'Inserting incremental order items...';

    INSERT INTO dbo.OrderItems
    (
        OrderId,
        ProductId,
        Quantity,
        UnitPrice,
        LineAmount,
        CreatedAt,
        UpdatedAt
    )
    VALUES
    (
        @OrderId,
        @ProductId1,
        @Quantity1,
        @UnitPrice1,
        @Quantity1 * @UnitPrice1,
        @ScenarioTimestamp,
        @ScenarioTimestamp
    ),
    (
        @OrderId,
        @ProductId2,
        @Quantity2,
        @UnitPrice2,
        @Quantity2 * @UnitPrice2,
        @ScenarioTimestamp,
        @ScenarioTimestamp
    );

    PRINT 'Inserting incremental payment...';

    INSERT INTO dbo.Payments
    (
        OrderId,
        PaymentDate,
        PaymentMethod,
        PaymentStatus,
        PaymentAmount,
        CurrencyCode,
        TransactionReference,
        CreatedAt,
        UpdatedAt
    )
    VALUES
    (
        @OrderId,
        DATEADD(MINUTE, 5, @ScenarioTimestamp),
        'CARD',
        'PENDING',
        @OrderTotal,
        'MXN',
        'TXN-INC-0001',
        DATEADD(MINUTE, 5, @ScenarioTimestamp),
        DATEADD(MINUTE, 5, @ScenarioTimestamp)
    );

    COMMIT TRANSACTION;

    PRINT 'Incremental insert scenario completed successfully.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT 'Incremental insert scenario failed. Transaction rolled back.';

    THROW;
END CATCH;
GO

/*
===============================================================================
Validation — Incremental insert scenario state
===============================================================================
*/

SELECT
    'Customers' AS SourceObjectName,
    COUNT(*) AS Scenario_Row_Count,
    MAX(UpdatedAt) AS Scenario_Max_UpdatedAt
FROM dbo.Customers
WHERE CustomerCode = 'CUST-INC-0001'

UNION ALL

SELECT
    'Orders',
    COUNT(*),
    MAX(UpdatedAt)
FROM dbo.Orders
WHERE OrderNumber = 'ORD-INC-0001'

UNION ALL

SELECT
    'OrderItems',
    COUNT(*),
    MAX(oi.UpdatedAt)
FROM dbo.OrderItems AS oi
INNER JOIN dbo.Orders AS o
    ON oi.OrderId = o.OrderId
WHERE o.OrderNumber = 'ORD-INC-0001'

UNION ALL

SELECT
    'Payments',
    COUNT(*),
    MAX(p.UpdatedAt)
FROM dbo.Payments AS p
INNER JOIN dbo.Orders AS o
    ON p.OrderId = o.OrderId
WHERE o.OrderNumber = 'ORD-INC-0001'
ORDER BY
    SourceObjectName;
GO