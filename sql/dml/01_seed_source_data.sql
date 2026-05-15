/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         01_seed_source_data.sql
Purpose:        Insert deterministic sample data into the local SQL Server
                source tables used by the ADF incremental ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 2 — SQL Server Local Setup

Source tables:
- dbo.Customers
- dbo.Products
- dbo.Orders
- dbo.OrderItems
- dbo.Payments

Expected initial volume:
- Customers:   20
- Products:    20
- Orders:      50
- OrderItems: 100
- Payments:    50

Notes:
- This script is intentionally non-destructive.
- It inserts seed data only if all source tables are empty.
- CreatedAt and UpdatedAt are populated with deterministic timestamps.
- UpdatedAt will be used later as the datetime-based watermark column.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

IF EXISTS (SELECT 1 FROM dbo.Customers)
   OR EXISTS (SELECT 1 FROM dbo.Products)
   OR EXISTS (SELECT 1 FROM dbo.Orders)
   OR EXISTS (SELECT 1 FROM dbo.OrderItems)
   OR EXISTS (SELECT 1 FROM dbo.Payments)
BEGIN
    PRINT 'One or more source tables already contain data. No seed data inserted.';
END
ELSE
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT 'Inserting seed data into [dbo].[Customers]...';

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
        ('CUST-0001', 'Ana',      'Garcia',     'ana.garcia@example.com',      '8441000001', 'Saltillo',   'Coahuila',      'MX', 1, '2026-05-01T08:00:00.000', '2026-05-01T08:00:00.000'),
        ('CUST-0002', 'Luis',     'Martinez',   'luis.martinez@example.com',   '8441000002', 'Monterrey',  'Nuevo Leon',    'MX', 1, '2026-05-01T08:05:00.000', '2026-05-01T08:05:00.000'),
        ('CUST-0003', 'Maria',    'Rodriguez',  'maria.rodriguez@example.com', '8441000003', 'Torreon',    'Coahuila',      'MX', 1, '2026-05-01T08:10:00.000', '2026-05-01T08:10:00.000'),
        ('CUST-0004', 'Carlos',   'Hernandez',  'carlos.hernandez@example.com','8441000004', 'Guadalajara','Jalisco',       'MX', 1, '2026-05-01T08:15:00.000', '2026-05-01T08:15:00.000'),
        ('CUST-0005', 'Sofia',    'Lopez',      'sofia.lopez@example.com',     '8441000005', 'CDMX',       'CDMX',          'MX', 1, '2026-05-01T08:20:00.000', '2026-05-01T08:20:00.000'),
        ('CUST-0006', 'Diego',    'Perez',      'diego.perez@example.com',     '8441000006', 'Saltillo',   'Coahuila',      'MX', 1, '2026-05-01T08:25:00.000', '2026-05-01T08:25:00.000'),
        ('CUST-0007', 'Valeria',  'Sanchez',    'valeria.sanchez@example.com', '8441000007', 'Puebla',     'Puebla',        'MX', 1, '2026-05-01T08:30:00.000', '2026-05-01T08:30:00.000'),
        ('CUST-0008', 'Fernando', 'Ramirez',    'fernando.ramirez@example.com','8441000008', 'Queretaro',  'Queretaro',     'MX', 1, '2026-05-01T08:35:00.000', '2026-05-01T08:35:00.000'),
        ('CUST-0009', 'Camila',   'Torres',     'camila.torres@example.com',   '8441000009', 'Merida',     'Yucatan',       'MX', 1, '2026-05-01T08:40:00.000', '2026-05-01T08:40:00.000'),
        ('CUST-0010', 'Jorge',    'Flores',     'jorge.flores@example.com',    '8441000010', 'Leon',       'Guanajuato',    'MX', 1, '2026-05-01T08:45:00.000', '2026-05-01T08:45:00.000'),
        ('CUST-0011', 'Lucia',    'Castillo',   'lucia.castillo@example.com',  '8441000011', 'Saltillo',   'Coahuila',      'MX', 1, '2026-05-01T08:50:00.000', '2026-05-01T08:50:00.000'),
        ('CUST-0012', 'Miguel',   'Vargas',     'miguel.vargas@example.com',   '8441000012', 'Monterrey',  'Nuevo Leon',    'MX', 1, '2026-05-01T08:55:00.000', '2026-05-01T08:55:00.000'),
        ('CUST-0013', 'Regina',   'Morales',    'regina.morales@example.com',  '8441000013', 'Torreon',    'Coahuila',      'MX', 1, '2026-05-01T09:00:00.000', '2026-05-01T09:00:00.000'),
        ('CUST-0014', 'Andres',   'Reyes',      'andres.reyes@example.com',    '8441000014', 'Guadalajara','Jalisco',       'MX', 1, '2026-05-01T09:05:00.000', '2026-05-01T09:05:00.000'),
        ('CUST-0015', 'Paola',    'Cruz',       'paola.cruz@example.com',      '8441000015', 'CDMX',       'CDMX',          'MX', 1, '2026-05-01T09:10:00.000', '2026-05-01T09:10:00.000'),
        ('CUST-0016', 'Ricardo',  'Ortiz',      'ricardo.ortiz@example.com',   '8441000016', 'Saltillo',   'Coahuila',      'MX', 1, '2026-05-01T09:15:00.000', '2026-05-01T09:15:00.000'),
        ('CUST-0017', 'Natalia',  'Mendoza',    'natalia.mendoza@example.com', '8441000017', 'Puebla',     'Puebla',        'MX', 1, '2026-05-01T09:20:00.000', '2026-05-01T09:20:00.000'),
        ('CUST-0018', 'Hector',   'Rios',       'hector.rios@example.com',     '8441000018', 'Queretaro',  'Queretaro',     'MX', 1, '2026-05-01T09:25:00.000', '2026-05-01T09:25:00.000'),
        ('CUST-0019', 'Elena',    'Navarro',    'elena.navarro@example.com',   '8441000019', 'Merida',     'Yucatan',       'MX', 1, '2026-05-01T09:30:00.000', '2026-05-01T09:30:00.000'),
        ('CUST-0020', 'Roberto',  'Medina',     'roberto.medina@example.com',  '8441000020', 'Leon',       'Guanajuato',    'MX', 1, '2026-05-01T09:35:00.000', '2026-05-01T09:35:00.000');

        PRINT 'Inserting seed data into [dbo].[Products]...';

        INSERT INTO dbo.Products
        (
            ProductSku,
            ProductName,
            Category,
            UnitPrice,
            CurrencyCode,
            IsActive,
            CreatedAt,
            UpdatedAt
        )
        VALUES
        ('SKU-0001', 'Purified Water 20L',          'Water',       45.00,  'MXN', 1, '2026-05-01T10:00:00.000', '2026-05-01T10:00:00.000'),
        ('SKU-0002', 'Purified Water 10L',          'Water',       28.00,  'MXN', 1, '2026-05-01T10:05:00.000', '2026-05-01T10:05:00.000'),
        ('SKU-0003', 'Ice Bag 5kg',                 'Ice',         38.00,  'MXN', 1, '2026-05-01T10:10:00.000', '2026-05-01T10:10:00.000'),
        ('SKU-0004', 'Ice Bag 10kg',                'Ice',         70.00,  'MXN', 1, '2026-05-01T10:15:00.000', '2026-05-01T10:15:00.000'),
        ('SKU-0005', 'Bottle Water 500ml Pack',     'Water',       95.00,  'MXN', 1, '2026-05-01T10:20:00.000', '2026-05-01T10:20:00.000'),
        ('SKU-0006', 'Bottle Water 1L Pack',        'Water',       120.00, 'MXN', 1, '2026-05-01T10:25:00.000', '2026-05-01T10:25:00.000'),
        ('SKU-0007', 'Glass Bottle 355ml Pack',     'Water',       160.00, 'MXN', 1, '2026-05-01T10:30:00.000', '2026-05-01T10:30:00.000'),
        ('SKU-0008', 'Sparkling Water Pack',        'Water',       180.00, 'MXN', 1, '2026-05-01T10:35:00.000', '2026-05-01T10:35:00.000'),
        ('SKU-0009', 'Restaurant Water Supply',     'Service',     250.00, 'MXN', 1, '2026-05-01T10:40:00.000', '2026-05-01T10:40:00.000'),
        ('SKU-0010', 'Cafe Water Supply',           'Service',     220.00, 'MXN', 1, '2026-05-01T10:45:00.000', '2026-05-01T10:45:00.000'),
        ('SKU-0011', 'Premium Water Dispenser',     'Equipment',   750.00, 'MXN', 1, '2026-05-01T10:50:00.000', '2026-05-01T10:50:00.000'),
        ('SKU-0012', 'Basic Water Dispenser',       'Equipment',   520.00, 'MXN', 1, '2026-05-01T10:55:00.000', '2026-05-01T10:55:00.000'),
        ('SKU-0013', 'Reusable Bottle 1L',          'Accessory',   85.00,  'MXN', 1, '2026-05-01T11:00:00.000', '2026-05-01T11:00:00.000'),
        ('SKU-0014', 'Reusable Bottle 750ml',       'Accessory',   75.00,  'MXN', 1, '2026-05-01T11:05:00.000', '2026-05-01T11:05:00.000'),
        ('SKU-0015', 'Emergency Water Kit',         'Water',       310.00, 'MXN', 1, '2026-05-01T11:10:00.000', '2026-05-01T11:10:00.000'),
        ('SKU-0016', 'Bulk Ice Supply',             'Ice',         450.00, 'MXN', 1, '2026-05-01T11:15:00.000', '2026-05-01T11:15:00.000'),
        ('SKU-0017', 'Event Water Package',         'Service',     600.00, 'MXN', 1, '2026-05-01T11:20:00.000', '2026-05-01T11:20:00.000'),
        ('SKU-0018', 'Office Water Subscription',   'Service',     390.00, 'MXN', 1, '2026-05-01T11:25:00.000', '2026-05-01T11:25:00.000'),
        ('SKU-0019', 'Restaurant Ice Subscription', 'Service',     480.00, 'MXN', 1, '2026-05-01T11:30:00.000', '2026-05-01T11:30:00.000'),
        ('SKU-0020', 'Monthly Water Plan',          'Service',     690.00, 'MXN', 1, '2026-05-01T11:35:00.000', '2026-05-01T11:35:00.000');

        PRINT 'Creating deterministic orders, order items, and payments...';

        IF OBJECT_ID(N'tempdb..#CustomerMap') IS NOT NULL
            DROP TABLE #CustomerMap;

        IF OBJECT_ID(N'tempdb..#ProductMap') IS NOT NULL
            DROP TABLE #ProductMap;

        SELECT
            ROW_NUMBER() OVER (ORDER BY CustomerId) AS CustomerSeq,
            CustomerId
        INTO #CustomerMap
        FROM dbo.Customers;

        SELECT
            ROW_NUMBER() OVER (ORDER BY ProductId) AS ProductSeq,
            ProductId,
            UnitPrice
        INTO #ProductMap
        FROM dbo.Products;

        DECLARE
            @i                 int = 1,
            @CustomerId        int,
            @OrderId           bigint,
            @OrderNumber       varchar(50),
            @OrderDate         datetime2(3),
            @CreatedAt         datetime2(3),
            @UpdatedAt         datetime2(3),
            @OrderStatus       varchar(30),
            @SalesChannel      varchar(50),
            @PaymentMethod     varchar(50),
            @PaymentStatus     varchar(30),
            @ProductId1        int,
            @ProductId2        int,
            @UnitPrice1        decimal(18,2),
            @UnitPrice2        decimal(18,2),
            @Quantity1         int,
            @Quantity2         int,
            @OrderTotal        decimal(18,2);

        WHILE @i <= 50
        BEGIN
            SELECT
                @CustomerId = CustomerId
            FROM #CustomerMap
            WHERE CustomerSeq = ((@i - 1) % 20) + 1;

            SELECT
                @ProductId1 = ProductId,
                @UnitPrice1 = UnitPrice
            FROM #ProductMap
            WHERE ProductSeq = ((@i - 1) % 20) + 1;

            SELECT
                @ProductId2 = ProductId,
                @UnitPrice2 = UnitPrice
            FROM #ProductMap
            WHERE ProductSeq = ((@i + 6) % 20) + 1;

            SET @OrderNumber = 'ORD-' + RIGHT('00000' + CAST(@i AS varchar(5)), 5);

            SET @OrderDate = DATEADD(MINUTE, @i * 15, CAST('2026-05-02T08:00:00.000' AS datetime2(3)));
            SET @CreatedAt = @OrderDate;
            SET @UpdatedAt = DATEADD(MINUTE, 5, @OrderDate);

            SET @OrderStatus =
                CASE
                    WHEN @i % 10 = 0 THEN 'CANCELLED'
                    WHEN @i % 7  = 0 THEN 'COMPLETED'
                    WHEN @i % 5  = 0 THEN 'SHIPPED'
                    WHEN @i % 2  = 0 THEN 'PAID'
                    ELSE 'CREATED'
                END;

            SET @SalesChannel =
                CASE
                    WHEN @i % 4 = 0 THEN 'MARKETPLACE'
                    WHEN @i % 3 = 0 THEN 'PHONE'
                    WHEN @i % 2 = 0 THEN 'STORE'
                    ELSE 'ONLINE'
                END;

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
                @OrderNumber,
                @CustomerId,
                @OrderDate,
                @OrderStatus,
                0.00,
                'MXN',
                @SalesChannel,
                @CreatedAt,
                @UpdatedAt
            );

            SET @OrderId = CONVERT(bigint, SCOPE_IDENTITY());

            SET @Quantity1 = (@i % 3) + 1;
            SET @Quantity2 = ((@i + 1) % 2) + 1;

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
                @CreatedAt,
                @UpdatedAt
            ),
            (
                @OrderId,
                @ProductId2,
                @Quantity2,
                @UnitPrice2,
                @Quantity2 * @UnitPrice2,
                @CreatedAt,
                @UpdatedAt
            );

            SELECT
                @OrderTotal = SUM(LineAmount)
            FROM dbo.OrderItems
            WHERE OrderId = @OrderId;

            UPDATE dbo.Orders
            SET
                OrderTotal = @OrderTotal,
                UpdatedAt = @UpdatedAt
            WHERE OrderId = @OrderId;

            SET @PaymentStatus =
                CASE
                    WHEN @OrderStatus = 'CANCELLED' THEN 'DECLINED'
                    WHEN @OrderStatus = 'CREATED'   THEN 'PENDING'
                    ELSE 'APPROVED'
                END;

            SET @PaymentMethod =
                CASE
                    WHEN @i % 4 = 0 THEN 'TRANSFER'
                    WHEN @i % 3 = 0 THEN 'PAYPAL'
                    WHEN @i % 2 = 0 THEN 'CASH'
                    ELSE 'CARD'
                END;

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
                DATEADD(MINUTE, 20, @OrderDate),
                @PaymentMethod,
                @PaymentStatus,
                @OrderTotal,
                'MXN',
                'TXN-' + RIGHT('00000' + CAST(@i AS varchar(5)), 5),
                DATEADD(MINUTE, 20, @OrderDate),
                DATEADD(MINUTE, 25, @OrderDate)
            );

            SET @i += 1;
        END;

        COMMIT TRANSACTION;

        PRINT 'Seed data inserted successfully.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT 'Error inserting seed data. Transaction rolled back.';

        THROW;
    END CATCH;
END;
GO

/*
===============================================================================
Validation 1 — Initial row counts
===============================================================================
*/

SELECT
    'Customers' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.Customers

UNION ALL

SELECT
    'Products' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.Products

UNION ALL

SELECT
    'Orders' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.Orders

UNION ALL

SELECT
    'OrderItems' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.OrderItems

UNION ALL

SELECT
    'Payments' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.Payments
ORDER BY
    TableName;
GO

/*
===============================================================================
Validation 2 — UpdatedAt ranges
===============================================================================
*/

SELECT
    'Customers' AS TableName,
    MIN(UpdatedAt) AS MinUpdatedAt,
    MAX(UpdatedAt) AS MaxUpdatedAt,
    COUNT(*) AS Row_Count
FROM dbo.Customers

UNION ALL

SELECT
    'Products',
    MIN(UpdatedAt),
    MAX(UpdatedAt),
    COUNT(*)
FROM dbo.Products

UNION ALL

SELECT
    'Orders',
    MIN(UpdatedAt),
    MAX(UpdatedAt),
    COUNT(*)
FROM dbo.Orders

UNION ALL

SELECT
    'OrderItems',
    MIN(UpdatedAt),
    MAX(UpdatedAt),
    COUNT(*)
FROM dbo.OrderItems

UNION ALL

SELECT
    'Payments',
    MIN(UpdatedAt),
    MAX(UpdatedAt),
    COUNT(*)
FROM dbo.Payments
ORDER BY
    TableName;
GO

/*
===============================================================================
Validation 3 — Order status distribution
===============================================================================
*/

SELECT
    OrderStatus,
    COUNT(*) AS OrdersCount
FROM dbo.Orders
GROUP BY
    OrderStatus
ORDER BY
    OrderStatus;
GO

/*
===============================================================================
Validation 4 — Payment status distribution
===============================================================================
*/

SELECT
    PaymentStatus,
    COUNT(*) AS PaymentsCount
FROM dbo.Payments
GROUP BY
    PaymentStatus
ORDER BY
    PaymentStatus;
GO

/*
===============================================================================
Validation 5 — Source relationship sample
===============================================================================
*/

SELECT TOP (20)
    o.OrderNumber,
    c.CustomerCode,
    CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
    o.OrderStatus,
    o.OrderTotal,
    p.PaymentStatus,
    p.PaymentAmount,
    o.CreatedAt AS OrderCreatedAt,
    o.UpdatedAt AS OrderUpdatedAt,
    p.UpdatedAt AS PaymentUpdatedAt
FROM dbo.Orders AS o
INNER JOIN dbo.Customers AS c
    ON o.CustomerId = c.CustomerId
LEFT JOIN dbo.Payments AS p
    ON o.OrderId = p.OrderId
ORDER BY
    o.OrderId;
GO