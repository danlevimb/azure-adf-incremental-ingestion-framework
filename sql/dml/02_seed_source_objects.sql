/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         02_seed_source_objects.sql
Purpose:        Seed source object metadata for SQL Server tables and CSV/JSON
                file sources used by the ADF incremental ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Control tables:
- ctl.SourceObject
- ctl.FileSourceConfig

Notes:
- This script is intentionally non-destructive.
- It inserts metadata records only if they do not already exist.
- SQL source objects use UpdatedAt as the initial datetime watermark.
- File sources are configured as FULL loads for the MVP.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

/*
===============================================================================
1. Seed SQL Server source objects
===============================================================================
*/

DECLARE @InitialWatermark datetime2(3) = '1900-01-01T00:00:00.000';

PRINT 'Seeding SQL Server source objects...';

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'sales_local'
      AND SourceType = 'SQL_TABLE'
      AND SourceSchema = 'dbo'
      AND SourceObjectName = 'Customers'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'sales_local',
        'SQL_TABLE',
        'dbo',
        'Customers',
        'UpdatedAt',
        @InitialWatermark,
        'INCREMENTAL',
        1,
        'bronze',
        'sqlserver/sales_local/dbo/customers',
        'PARQUET'
    );

    PRINT 'Inserted SourceObject for dbo.Customers.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'sales_local'
      AND SourceType = 'SQL_TABLE'
      AND SourceSchema = 'dbo'
      AND SourceObjectName = 'Products'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'sales_local',
        'SQL_TABLE',
        'dbo',
        'Products',
        'UpdatedAt',
        @InitialWatermark,
        'INCREMENTAL',
        1,
        'bronze',
        'sqlserver/sales_local/dbo/products',
        'PARQUET'
    );

    PRINT 'Inserted SourceObject for dbo.Products.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'sales_local'
      AND SourceType = 'SQL_TABLE'
      AND SourceSchema = 'dbo'
      AND SourceObjectName = 'Orders'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'sales_local',
        'SQL_TABLE',
        'dbo',
        'Orders',
        'UpdatedAt',
        @InitialWatermark,
        'INCREMENTAL',
        1,
        'bronze',
        'sqlserver/sales_local/dbo/orders',
        'PARQUET'
    );

    PRINT 'Inserted SourceObject for dbo.Orders.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'sales_local'
      AND SourceType = 'SQL_TABLE'
      AND SourceSchema = 'dbo'
      AND SourceObjectName = 'OrderItems'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'sales_local',
        'SQL_TABLE',
        'dbo',
        'OrderItems',
        'UpdatedAt',
        @InitialWatermark,
        'INCREMENTAL',
        1,
        'bronze',
        'sqlserver/sales_local/dbo/orderitems',
        'PARQUET'
    );

    PRINT 'Inserted SourceObject for dbo.OrderItems.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'sales_local'
      AND SourceType = 'SQL_TABLE'
      AND SourceSchema = 'dbo'
      AND SourceObjectName = 'Payments'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'sales_local',
        'SQL_TABLE',
        'dbo',
        'Payments',
        'UpdatedAt',
        @InitialWatermark,
        'INCREMENTAL',
        1,
        'bronze',
        'sqlserver/sales_local/dbo/payments',
        'PARQUET'
    );

    PRINT 'Inserted SourceObject for dbo.Payments.';
END;
GO

/*
===============================================================================
2. Seed file source objects
===============================================================================
*/

PRINT 'Seeding CSV and JSON source objects...';

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceType = 'CSV_FILE'
      AND SourceObjectName = 'currency_rates'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'reference_files',
        'CSV_FILE',
        NULL,
        'currency_rates',
        NULL,
        NULL,
        'FULL',
        1,
        'bronze',
        'files/csv/currency_rates',
        'CSV'
    );

    PRINT 'Inserted SourceObject for currency_rates CSV.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceType = 'CSV_FILE'
      AND SourceObjectName = 'country_currency'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'reference_files',
        'CSV_FILE',
        NULL,
        'country_currency',
        NULL,
        NULL,
        'FULL',
        1,
        'bronze',
        'files/csv/country_currency',
        'CSV'
    );

    PRINT 'Inserted SourceObject for country_currency CSV.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceType = 'JSON_FILE'
      AND SourceObjectName = 'source_system_metadata'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'reference_files',
        'JSON_FILE',
        NULL,
        'source_system_metadata',
        NULL,
        NULL,
        'FULL',
        1,
        'bronze',
        'files/json/source_system_metadata',
        'JSON'
    );

    PRINT 'Inserted SourceObject for source_system_metadata JSON.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceType = 'JSON_FILE'
      AND SourceObjectName = 'manual_adjustments'
)
BEGIN
    INSERT INTO ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        WatermarkColumn,
        LastWatermarkValue,
        LoadType,
        IsActive,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    )
    VALUES
    (
        'reference_files',
        'JSON_FILE',
        NULL,
        'manual_adjustments',
        NULL,
        NULL,
        'FULL',
        1,
        'bronze',
        'files/json/manual_adjustments',
        'JSON'
    );

    PRINT 'Inserted SourceObject for manual_adjustments JSON.';
END;
GO

/*
===============================================================================
3. Seed file source configuration
===============================================================================
*/

PRINT 'Seeding file source configuration...';

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.FileSourceConfig AS f
    INNER JOIN ctl.SourceObject AS s
        ON f.SourceObjectId = s.SourceObjectId
    WHERE s.SourceSystemName = 'reference_files'
      AND s.SourceObjectName = 'currency_rates'
)
BEGIN
    INSERT INTO ctl.FileSourceConfig
    (
        SourceObjectId,
        FileFormat,
        SourcePath,
        FileNamePattern,
        HasHeader,
        Delimiter,
        IsActive,
        DestinationFolder
    )
    SELECT
        SourceObjectId,
        'CSV',
        'landing/files/csv/currency_rates',
        'currency_rates*.csv',
        1,
        ',',
        1,
        'files/csv/currency_rates'
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceObjectName = 'currency_rates';

    PRINT 'Inserted FileSourceConfig for currency_rates.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.FileSourceConfig AS f
    INNER JOIN ctl.SourceObject AS s
        ON f.SourceObjectId = s.SourceObjectId
    WHERE s.SourceSystemName = 'reference_files'
      AND s.SourceObjectName = 'country_currency'
)
BEGIN
    INSERT INTO ctl.FileSourceConfig
    (
        SourceObjectId,
        FileFormat,
        SourcePath,
        FileNamePattern,
        HasHeader,
        Delimiter,
        IsActive,
        DestinationFolder
    )
    SELECT
        SourceObjectId,
        'CSV',
        'landing/files/csv/country_currency',
        'country_currency*.csv',
        1,
        ',',
        1,
        'files/csv/country_currency'
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceObjectName = 'country_currency';

    PRINT 'Inserted FileSourceConfig for country_currency.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.FileSourceConfig AS f
    INNER JOIN ctl.SourceObject AS s
        ON f.SourceObjectId = s.SourceObjectId
    WHERE s.SourceSystemName = 'reference_files'
      AND s.SourceObjectName = 'source_system_metadata'
)
BEGIN
    INSERT INTO ctl.FileSourceConfig
    (
        SourceObjectId,
        FileFormat,
        SourcePath,
        FileNamePattern,
        HasHeader,
        Delimiter,
        IsActive,
        DestinationFolder
    )
    SELECT
        SourceObjectId,
        'JSON',
        'landing/files/json/source_system_metadata',
        'source_system_metadata*.json',
        NULL,
        NULL,
        1,
        'files/json/source_system_metadata'
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceObjectName = 'source_system_metadata';

    PRINT 'Inserted FileSourceConfig for source_system_metadata.';
END;

IF NOT EXISTS
(
    SELECT 1
    FROM ctl.FileSourceConfig AS f
    INNER JOIN ctl.SourceObject AS s
        ON f.SourceObjectId = s.SourceObjectId
    WHERE s.SourceSystemName = 'reference_files'
      AND s.SourceObjectName = 'manual_adjustments'
)
BEGIN
    INSERT INTO ctl.FileSourceConfig
    (
        SourceObjectId,
        FileFormat,
        SourcePath,
        FileNamePattern,
        HasHeader,
        Delimiter,
        IsActive,
        DestinationFolder
    )
    SELECT
        SourceObjectId,
        'JSON',
        'landing/files/json/manual_adjustments',
        'manual_adjustments*.json',
        NULL,
        NULL,
        1,
        'files/json/manual_adjustments'
    FROM ctl.SourceObject
    WHERE SourceSystemName = 'reference_files'
      AND SourceObjectName = 'manual_adjustments';

    PRINT 'Inserted FileSourceConfig for manual_adjustments.';
END;
GO

/*
===============================================================================
4. Validation — SourceObject metadata
===============================================================================
*/

SELECT
    SourceObjectId,
    SourceSystemName,
    SourceType,
    SourceSchema,
    SourceObjectName,
    WatermarkColumn,
    LastWatermarkValue,
    LoadType,
    IsActive,
    DestinationContainer,
    DestinationFolder,
    DestinationFormat
FROM ctl.SourceObject
ORDER BY
    SourceType,
    SourceObjectName;
GO

/*
===============================================================================
5. Validation — SQL source watermarks
===============================================================================
*/

SELECT
    SourceObjectId,
    SourceSystemName,
    SourceSchema,
    SourceObjectName,
    WatermarkColumn,
    LastWatermarkValue,
    LoadType,
    DestinationContainer,
    DestinationFolder,
    DestinationFormat
FROM ctl.SourceObject
WHERE SourceType = 'SQL_TABLE'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
6. Validation — File source configuration
===============================================================================
*/

SELECT
    s.SourceObjectId,
    s.SourceSystemName,
    s.SourceType,
    s.SourceObjectName,
    s.DestinationContainer,
    s.DestinationFolder AS SourceObjectDestinationFolder,
    s.DestinationFormat,
    f.FileSourceConfigId,
    f.FileFormat,
    f.SourcePath,
    f.FileNamePattern,
    f.HasHeader,
    f.Delimiter,
    f.IsActive,
    f.DestinationFolder AS FileConfigDestinationFolder
FROM ctl.SourceObject AS s
INNER JOIN ctl.FileSourceConfig AS f
    ON s.SourceObjectId = f.SourceObjectId
ORDER BY
    s.SourceType,
    s.SourceObjectName;
GO

/*
===============================================================================
7. Validation — Source object counts
===============================================================================
*/

SELECT
    SourceType,
    COUNT(*) AS SourceObject_Count
FROM ctl.SourceObject
GROUP BY
    SourceType
ORDER BY
    SourceType;
GO