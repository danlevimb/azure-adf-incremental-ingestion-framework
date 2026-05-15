/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         01_validate_source_counts.sql
Purpose:        Validate source table row counts, UpdatedAt ranges, status
                distributions, and source relationships after seed data load.

Database:       ADF_Ingestion_Source
Phase:          Phase 2 — SQL Server Local Setup
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
Validation 1 — Source table row counts
===============================================================================
*/

SELECT
    'Customers' AS TableName,
    COUNT(*) AS Row_Count
FROM dbo.Customers

UNION ALL

SELECT
    'Products',
    COUNT(*)
FROM dbo.Products

UNION ALL

SELECT
    'Orders',
    COUNT(*)
FROM dbo.Orders

UNION ALL

SELECT
    'OrderItems',
    COUNT(*)
FROM dbo.OrderItems

UNION ALL

SELECT
    'Payments',
    COUNT(*)
FROM dbo.Payments
ORDER BY
    TableName;
GO

/*
===============================================================================
Validation 2 — Expected vs actual row counts
===============================================================================
*/

WITH ExpectedCounts AS
(
    SELECT 'Customers' AS TableName, 20 AS Expected_Row_Count
    UNION ALL SELECT 'Products', 20
    UNION ALL SELECT 'Orders', 50
    UNION ALL SELECT 'OrderItems', 100
    UNION ALL SELECT 'Payments', 50
),
ActualCounts AS
(
    SELECT 'Customers' AS TableName, COUNT(*) AS Actual_Row_Count FROM dbo.Customers
    UNION ALL SELECT 'Products', COUNT(*) FROM dbo.Products
    UNION ALL SELECT 'Orders', COUNT(*) FROM dbo.Orders
    UNION ALL SELECT 'OrderItems', COUNT(*) FROM dbo.OrderItems
    UNION ALL SELECT 'Payments', COUNT(*) FROM dbo.Payments
)
SELECT
    e.TableName,
    e.Expected_Row_Count,
    a.Actual_Row_Count,
    CASE
        WHEN e.Expected_Row_Count = a.Actual_Row_Count THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ExpectedCounts AS e
INNER JOIN ActualCounts AS a
    ON e.TableName = a.TableName
ORDER BY
    e.TableName;
GO

/*
===============================================================================
Validation 3 — UpdatedAt ranges
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
Validation 4 — Order status distribution
===============================================================================
*/

SELECT
    OrderStatus,
    COUNT(*) AS Order_Count
FROM dbo.Orders
GROUP BY
    OrderStatus
ORDER BY
    OrderStatus;
GO

/*
===============================================================================
Validation 5 — Payment status distribution
===============================================================================
*/

SELECT
    PaymentStatus,
    COUNT(*) AS Payment_Count
FROM dbo.Payments
GROUP BY
    PaymentStatus
ORDER BY
    PaymentStatus;
GO

/*
===============================================================================
Validation 6 — Source relationship sample
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