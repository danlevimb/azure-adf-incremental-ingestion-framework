/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         02_usp_GetSourceObjectConfig.sql
Purpose:        Return full configuration for a single source object.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_GetSourceObjectConfig

Used by:
- PL_01_SQL_Incremental_Ingestion
- PL_02_File_Ingestion

Notes:
- This procedure returns SQL source configuration and, when applicable,
  file-specific configuration from ctl.FileSourceConfig.
- It is intended to be called by ADF using SourceObjectId.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_GetSourceObjectConfig
(
    @SourceObjectId int
)
AS
BEGIN
    SET NOCOUNT ON;

    /*
    ===========================================================================
    Validate input
    ===========================================================================
    */

    IF @SourceObjectId IS NULL
    BEGIN
        THROW 50002, '@SourceObjectId cannot be NULL.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.SourceObject
        WHERE SourceObjectId = @SourceObjectId
    )
    BEGIN
        THROW 50003, 'SourceObjectId does not exist in ctl.SourceObject.', 1;
    END;

    /*
    ===========================================================================
    Return source object configuration
    ===========================================================================
    */

    SELECT
        s.SourceObjectId,
        s.SourceSystemName,
        s.SourceType,
        s.SourceSchema,
        s.SourceObjectName,
        s.WatermarkColumn,
        s.LastWatermarkValue,
        s.LoadType,
        s.IsActive,
        s.DestinationContainer,
        s.DestinationFolder,
        s.DestinationFormat,

        f.FileSourceConfigId,
        f.FileFormat,
        f.SourcePath,
        f.FileNamePattern,
        f.HasHeader,
        f.Delimiter,
        f.IsActive AS FileConfigIsActive,
        f.DestinationFolder AS FileConfigDestinationFolder
    FROM ctl.SourceObject AS s
    LEFT JOIN ctl.FileSourceConfig AS f
        ON s.SourceObjectId = f.SourceObjectId
    WHERE s.SourceObjectId = @SourceObjectId;
END;
GO

/*
===============================================================================
Validation 1 — SQL source config example
Compact evidence target: Orders
===============================================================================
*/

DECLARE @OrdersSourceObjectId int;

SELECT
    @OrdersSourceObjectId = SourceObjectId
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

EXEC ctl.usp_GetSourceObjectConfig
    @SourceObjectId = @OrdersSourceObjectId;
GO

/*
===============================================================================
Validation 2 — File source config example
Compact evidence target: currency_rates
===============================================================================
*/

DECLARE @CurrencyRatesSourceObjectId int;

SELECT
    @CurrencyRatesSourceObjectId = SourceObjectId
FROM ctl.SourceObject
WHERE SourceSystemName = 'reference_files'
  AND SourceType = 'CSV_FILE'
  AND SourceObjectName = 'currency_rates';

EXEC ctl.usp_GetSourceObjectConfig
    @SourceObjectId = @CurrencyRatesSourceObjectId;
GO

/*
===============================================================================
Validation 3 — Compact SQL evidence query
===============================================================================
*/

SELECT
    s.SourceObjectId,
    s.SourceType,
    s.SourceObjectName,
    s.WatermarkColumn,
    s.LastWatermarkValue,
    s.LoadType,
    s.DestinationFormat
FROM ctl.SourceObject AS s
WHERE s.SourceSystemName = 'sales_local'
  AND s.SourceType = 'SQL_TABLE'
  AND s.SourceObjectName = 'Orders';
GO

/*
===============================================================================
Validation 4 — Compact file evidence query
===============================================================================
*/

SELECT
    s.SourceObjectId,
    s.SourceType,
    s.SourceObjectName,
    f.FileFormat,
    f.SourcePath,
    f.FileNamePattern,
    f.HasHeader,
    f.Delimiter
FROM ctl.SourceObject AS s
INNER JOIN ctl.FileSourceConfig AS f
    ON s.SourceObjectId = f.SourceObjectId
WHERE s.SourceSystemName = 'reference_files'
  AND s.SourceObjectName = 'currency_rates';
GO