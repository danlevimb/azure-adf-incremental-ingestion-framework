/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         03_usp_GetCurrentHighWatermark.sql
Purpose:        Return the current high watermark value for a SQL Server source
                table using a configured datetime watermark column.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_GetCurrentHighWatermark

Used by:
- PL_01_SQL_Incremental_Ingestion

Notes:
- This procedure is read-only.
- It uses dynamic SQL safely with QUOTENAME().
- It returns MAX(<WatermarkColumn>) from the configured source table.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_GetCurrentHighWatermark
(
    @SourceSchema     sysname,
    @SourceObjectName sysname,
    @WatermarkColumn  sysname
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @SourceObjectFullName nvarchar(517),
        @Sql                  nvarchar(max),
        @CurrentHighWatermarkValue datetime2(3);

    /*
    ===========================================================================
    Validate inputs
    ===========================================================================
    */

    IF @SourceSchema IS NULL
       OR @SourceObjectName IS NULL
       OR @WatermarkColumn IS NULL
    BEGIN
        THROW 50004, '@SourceSchema, @SourceObjectName, and @WatermarkColumn are required.', 1;
    END;

    SET @SourceObjectFullName = QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceObjectName);

    IF OBJECT_ID(@SourceObjectFullName, N'U') IS NULL
    BEGIN
        THROW 50005, 'Source table does not exist.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(@SourceObjectFullName, N'U')
          AND c.name = @WatermarkColumn
    )
    BEGIN
        THROW 50006, 'Watermark column does not exist in the source table.', 1;
    END;

    /*
    ===========================================================================
    Calculate current high watermark
    ===========================================================================
    */

    SET @Sql = N'
        SELECT
            @CurrentHighWatermarkValue = MAX(' + QUOTENAME(@WatermarkColumn) + N')
        FROM ' + @SourceObjectFullName + N';';

    EXEC sys.sp_executesql
        @Sql,
        N'@CurrentHighWatermarkValue datetime2(3) OUTPUT',
        @CurrentHighWatermarkValue = @CurrentHighWatermarkValue OUTPUT;

    /*
    ===========================================================================
    Return result for ADF Lookup / Stored Procedure activity
    ===========================================================================
    */

    SELECT
        @SourceSchema AS SourceSchema,
        @SourceObjectName AS SourceObjectName,
        @WatermarkColumn AS WatermarkColumn,
        @CurrentHighWatermarkValue AS CurrentHighWatermarkValue;
END;
GO

/*
===============================================================================
Validation 1 — Get current high watermark for Orders
Compact evidence target
===============================================================================
*/

EXEC ctl.usp_GetCurrentHighWatermark
    @SourceSchema = 'dbo',
    @SourceObjectName = 'Orders',
    @WatermarkColumn = 'UpdatedAt';
GO

/*
===============================================================================
Validation 2 — Compare against direct MAX(UpdatedAt)
===============================================================================
*/

SELECT
    'dbo' AS SourceSchema,
    'Orders' AS SourceObjectName,
    'UpdatedAt' AS WatermarkColumn,
    MAX(UpdatedAt) AS DirectMaxUpdatedAt
FROM dbo.Orders;
GO