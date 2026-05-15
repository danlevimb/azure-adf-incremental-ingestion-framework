/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         02_create_source_tables.sql
Purpose:        Create source business tables for the local SQL Server source
                model used by the ADF incremental ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 2 — SQL Server Local Setup

Source tables:
- dbo.Customers
- dbo.Products
- dbo.Orders
- dbo.OrderItems
- dbo.Payments

Notes:
- This script is intentionally non-destructive.
- It creates tables only if they do not already exist.
- It does not drop or overwrite existing tables.
- All source tables include CreatedAt and UpdatedAt.
- UpdatedAt will be used as the initial datetime-based watermark column.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
1. dbo.Customers
===============================================================================
*/

IF OBJECT_ID(N'dbo.Customers', N'U') IS NULL
BEGIN
    PRINT 'Creating table [dbo].[Customers]...';

    CREATE TABLE dbo.Customers
    (
        CustomerId      int IDENTITY(1,1) NOT NULL,
        CustomerCode    varchar(30) NOT NULL,
        FirstName       varchar(100) NOT NULL,
        LastName        varchar(100) NOT NULL,
        Email           varchar(255) NOT NULL,
        Phone           varchar(30) NULL,
        City            varchar(100) NULL,
        StateName       varchar(100) NULL,
        CountryCode     char(2) NOT NULL,
        IsActive        bit NOT NULL
            CONSTRAINT DF_Customers_IsActive DEFAULT (1),
        CreatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Customers_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Customers_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_Customers
            PRIMARY KEY CLUSTERED (CustomerId),

        CONSTRAINT UQ_Customers_CustomerCode
            UNIQUE (CustomerCode),

        CONSTRAINT UQ_Customers_Email
            UNIQUE (Email)
    );

    PRINT 'Table [dbo].[Customers] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Customers] already exists. No action taken.';
END;
GO

/*
===============================================================================
2. dbo.Products
===============================================================================
*/

IF OBJECT_ID(N'dbo.Products', N'U') IS NULL
BEGIN
    PRINT 'Creating table [dbo].[Products]...';

    CREATE TABLE dbo.Products
    (
        ProductId       int IDENTITY(1,1) NOT NULL,
        ProductSku      varchar(50) NOT NULL,
        ProductName     varchar(150) NOT NULL,
        Category        varchar(100) NOT NULL,
        UnitPrice       decimal(18,2) NOT NULL,
        CurrencyCode    char(3) NOT NULL,
        IsActive        bit NOT NULL
            CONSTRAINT DF_Products_IsActive DEFAULT (1),
        CreatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Products_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Products_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_Products
            PRIMARY KEY CLUSTERED (ProductId),

        CONSTRAINT UQ_Products_ProductSku
            UNIQUE (ProductSku),

        CONSTRAINT CK_Products_UnitPrice
            CHECK (UnitPrice >= 0)
    );

    PRINT 'Table [dbo].[Products] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Products] already exists. No action taken.';
END;
GO

/*
===============================================================================
3. dbo.Orders
===============================================================================
*/

IF OBJECT_ID(N'dbo.Orders', N'U') IS NULL
BEGIN
    PRINT 'Creating table [dbo].[Orders]...';

    CREATE TABLE dbo.Orders
    (
        OrderId         bigint IDENTITY(1,1) NOT NULL,
        OrderNumber     varchar(50) NOT NULL,
        CustomerId      int NOT NULL,
        OrderDate       datetime2(3) NOT NULL,
        OrderStatus     varchar(30) NOT NULL,
        OrderTotal      decimal(18,2) NOT NULL,
        CurrencyCode    char(3) NOT NULL,
        SalesChannel    varchar(50) NOT NULL,
        CreatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Orders_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_Orders_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_Orders
            PRIMARY KEY CLUSTERED (OrderId),

        CONSTRAINT UQ_Orders_OrderNumber
            UNIQUE (OrderNumber),

        CONSTRAINT FK_Orders_Customers
            FOREIGN KEY (CustomerId)
            REFERENCES dbo.Customers (CustomerId),

        CONSTRAINT CK_Orders_OrderStatus
            CHECK (OrderStatus IN
            (
                'CREATED',
                'PAID',
                'CANCELLED',
                'SHIPPED',
                'COMPLETED',
                'REFUNDED'
            )),

        CONSTRAINT CK_Orders_OrderTotal
            CHECK (OrderTotal >= 0),

        CONSTRAINT CK_Orders_SalesChannel
            CHECK (SalesChannel IN
            (
                'ONLINE',
                'STORE',
                'PHONE',
                'MARKETPLACE'
            ))
    );

    PRINT 'Table [dbo].[Orders] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Orders] already exists. No action taken.';
END;
GO

/*
===============================================================================
4. dbo.OrderItems
===============================================================================
*/

IF OBJECT_ID(N'dbo.OrderItems', N'U') IS NULL
BEGIN
    PRINT 'Creating table [dbo].[OrderItems]...';

    CREATE TABLE dbo.OrderItems
    (
        OrderItemId     bigint IDENTITY(1,1) NOT NULL,
        OrderId         bigint NOT NULL,
        ProductId       int NOT NULL,
        Quantity        int NOT NULL,
        UnitPrice       decimal(18,2) NOT NULL,
        LineAmount      decimal(18,2) NOT NULL,
        CreatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_OrderItems_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_OrderItems_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_OrderItems
            PRIMARY KEY CLUSTERED (OrderItemId),

        CONSTRAINT FK_OrderItems_Orders
            FOREIGN KEY (OrderId)
            REFERENCES dbo.Orders (OrderId),

        CONSTRAINT FK_OrderItems_Products
            FOREIGN KEY (ProductId)
            REFERENCES dbo.Products (ProductId),

        CONSTRAINT CK_OrderItems_Quantity
            CHECK (Quantity > 0),

        CONSTRAINT CK_OrderItems_UnitPrice
            CHECK (UnitPrice >= 0),

        CONSTRAINT CK_OrderItems_LineAmount
            CHECK (LineAmount >= 0)
    );

    PRINT 'Table [dbo].[OrderItems] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[OrderItems] already exists. No action taken.';
END;
GO

/*
===============================================================================
5. dbo.Payments
===============================================================================
*/

IF OBJECT_ID(N'dbo.Payments', N'U') IS NULL
BEGIN
    PRINT 'Creating table [dbo].[Payments]...';

    CREATE TABLE dbo.Payments
    (
        PaymentId             bigint IDENTITY(1,1) NOT NULL,
        OrderId               bigint NOT NULL,
        PaymentDate           datetime2(3) NOT NULL,
        PaymentMethod         varchar(50) NOT NULL,
        PaymentStatus         varchar(30) NOT NULL,
        PaymentAmount         decimal(18,2) NOT NULL,
        CurrencyCode          char(3) NOT NULL,
        TransactionReference  varchar(100) NOT NULL,
        CreatedAt             datetime2(3) NOT NULL
            CONSTRAINT DF_Payments_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt             datetime2(3) NOT NULL
            CONSTRAINT DF_Payments_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_Payments
            PRIMARY KEY CLUSTERED (PaymentId),

        CONSTRAINT UQ_Payments_TransactionReference
            UNIQUE (TransactionReference),

        CONSTRAINT FK_Payments_Orders
            FOREIGN KEY (OrderId)
            REFERENCES dbo.Orders (OrderId),

        CONSTRAINT CK_Payments_PaymentMethod
            CHECK (PaymentMethod IN
            (
                'CARD',
                'CASH',
                'TRANSFER',
                'PAYPAL'
            )),

        CONSTRAINT CK_Payments_PaymentStatus
            CHECK (PaymentStatus IN
            (
                'PENDING',
                'APPROVED',
                'DECLINED',
                'REFUNDED'
            )),

        CONSTRAINT CK_Payments_PaymentAmount
            CHECK (PaymentAmount >= 0)
    );

    PRINT 'Table [dbo].[Payments] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [dbo].[Payments] already exists. No action taken.';
END;
GO

/*
===============================================================================
6. Supporting indexes for incremental ingestion and relationships
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Customers_UpdatedAt'
      AND object_id = OBJECT_ID(N'dbo.Customers')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Customers_UpdatedAt
    ON dbo.Customers (UpdatedAt);

    PRINT 'Index [IX_Customers_UpdatedAt] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Products_UpdatedAt'
      AND object_id = OBJECT_ID(N'dbo.Products')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Products_UpdatedAt
    ON dbo.Products (UpdatedAt);

    PRINT 'Index [IX_Products_UpdatedAt] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Orders_UpdatedAt'
      AND object_id = OBJECT_ID(N'dbo.Orders')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Orders_UpdatedAt
    ON dbo.Orders (UpdatedAt);

    PRINT 'Index [IX_Orders_UpdatedAt] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_OrderItems_UpdatedAt'
      AND object_id = OBJECT_ID(N'dbo.OrderItems')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_OrderItems_UpdatedAt
    ON dbo.OrderItems (UpdatedAt);

    PRINT 'Index [IX_OrderItems_UpdatedAt] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Payments_UpdatedAt'
      AND object_id = OBJECT_ID(N'dbo.Payments')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Payments_UpdatedAt
    ON dbo.Payments (UpdatedAt);

    PRINT 'Index [IX_Payments_UpdatedAt] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Orders_CustomerId'
      AND object_id = OBJECT_ID(N'dbo.Orders')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Orders_CustomerId
    ON dbo.Orders (CustomerId);

    PRINT 'Index [IX_Orders_CustomerId] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_OrderItems_OrderId'
      AND object_id = OBJECT_ID(N'dbo.OrderItems')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_OrderItems_OrderId
    ON dbo.OrderItems (OrderId);

    PRINT 'Index [IX_OrderItems_OrderId] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_OrderItems_ProductId'
      AND object_id = OBJECT_ID(N'dbo.OrderItems')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_OrderItems_ProductId
    ON dbo.OrderItems (ProductId);

    PRINT 'Index [IX_OrderItems_ProductId] created successfully.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_Payments_OrderId'
      AND object_id = OBJECT_ID(N'dbo.Payments')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_Payments_OrderId
    ON dbo.Payments (OrderId);

    PRINT 'Index [IX_Payments_OrderId] created successfully.';
END;
GO

/*
===============================================================================
7. Validation — Tables
===============================================================================
*/

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    t.create_date,
    t.modify_date
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
WHERE s.name = N'dbo'
  AND t.name IN
  (
      N'Customers',
      N'Products',
      N'Orders',
      N'OrderItems',
      N'Payments'
  )
ORDER BY
    t.name;
GO

/*
===============================================================================
8. Validation — Primary keys and foreign keys
===============================================================================
*/

SELECT
    fk.name AS ForeignKeyName,
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ChildSchema,
    OBJECT_NAME(fk.parent_object_id) AS ChildTable,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ParentSchema,
    OBJECT_NAME(fk.referenced_object_id) AS ParentTable
FROM sys.foreign_keys AS fk
WHERE fk.parent_object_id IN
(
    OBJECT_ID(N'dbo.Orders'),
    OBJECT_ID(N'dbo.OrderItems'),
    OBJECT_ID(N'dbo.Payments')
)
ORDER BY
    ChildTable,
    ForeignKeyName;
GO

/*
===============================================================================
9. Validation — UpdatedAt indexes
===============================================================================
*/

SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc
FROM sys.indexes AS i
WHERE i.name IN
(
    N'IX_Customers_UpdatedAt',
    N'IX_Products_UpdatedAt',
    N'IX_Orders_UpdatedAt',
    N'IX_OrderItems_UpdatedAt',
    N'IX_Payments_UpdatedAt'
)
ORDER BY
    TableName,
    IndexName;
GO