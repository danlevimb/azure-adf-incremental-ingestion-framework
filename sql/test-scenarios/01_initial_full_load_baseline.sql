/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         01_initial_full_load_baseline.sql
Purpose:        Capture the baseline state before the first ADF initial full
                load using the low initial watermark strategy.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer / Test Scenario Preparation

Scenario:
- Initial full load baseline

Notes:
- This script is read-only.
- It does not modify source data or control metadata.
- It captures the source row counts, current source high watermarks,
  and configured control table watermarks before the first ADF load.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
1. Source row counts before initial full load
===============================================================================
*/

SELECT
    'Customers' AS SourceObjectName,
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
    SourceObjectName;
GO

/*
===============================================================================
2. Source high watermarks before initial full load
===============================================================================
*/

SELECT
    'Customers' AS SourceObjectName,
    MAX(UpdatedAt) AS CurrentHighWatermarkValue
FROM dbo.Customers

UNION ALL

SELECT
    'Products',
    MAX(UpdatedAt)
FROM dbo.Products

UNION ALL

SELECT
    'Orders',
    MAX(UpdatedAt)
FROM dbo.Orders

UNION ALL

SELECT
    'OrderItems',
    MAX(UpdatedAt)
FROM dbo.OrderItems

UNION ALL

SELECT
    'Payments',
    MAX(UpdatedAt)
FROM dbo.Payments
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
3. Control table watermark state before initial full load
===============================================================================
*/

SELECT
    SourceObjectName,
    WatermarkColumn,
    LastWatermarkValue,
    LoadType,
    DestinationFormat
FROM ctl.SourceObject
WHERE SourceType = 'SQL_TABLE'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
4. Initial extraction eligibility preview
Expected:
- Because LastWatermarkValue = 1900-01-01, all existing rows are eligible.
===============================================================================
*/

WITH SourceCounts AS
(
    SELECT 'Customers' AS SourceObjectName, COUNT(*) AS Eligible_Row_Count
    FROM dbo.Customers
    WHERE UpdatedAt > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')

    UNION ALL

    SELECT 'Products', COUNT(*)
    FROM dbo.Products
    WHERE UpdatedAt > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')

    UNION ALL

    SELECT 'Orders', COUNT(*)
    FROM dbo.Orders
    WHERE UpdatedAt > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')

    UNION ALL

    SELECT 'OrderItems', COUNT(*)
    FROM dbo.OrderItems
    WHERE UpdatedAt > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')

    UNION ALL

    SELECT 'Payments', COUNT(*)
    FROM dbo.Payments
    WHERE UpdatedAt > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
)
SELECT
    SourceObjectName,
    Eligible_Row_Count
FROM SourceCounts
ORDER BY
    SourceObjectName;
GO