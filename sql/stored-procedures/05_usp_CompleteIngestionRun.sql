/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         05_usp_CompleteIngestionRun.sql
Purpose:        Mark an ingestion run as succeeded after a successful copy
                operation.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_CompleteIngestionRun

Used by:
- PL_01_SQL_Incremental_Ingestion
- PL_02_File_Ingestion

Notes:
- This procedure updates ctl.IngestionRun with Status = 'Succeeded'.
- It stores row counts, destination information, duration, and the new
  watermark candidate value.
- It does not update ctl.SourceObject.LastWatermarkValue.
- The actual watermark update is handled by ctl.usp_UpdateWatermark.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_CompleteIngestionRun
(
    @RunId                uniqueidentifier,
    @RowsRead             bigint = NULL,
    @RowsCopied           bigint = NULL,
    @NewWatermarkValue    datetime2(3) = NULL,
    @DestinationContainer varchar(100) = NULL,
    @DestinationFolder    varchar(500) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @EndedAt datetime2(3) = SYSUTCDATETIME();

    /*
    ===========================================================================
    Validate inputs
    ===========================================================================
    */

    IF @RunId IS NULL
    BEGIN
        THROW 50009, '@RunId cannot be NULL.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
    )
    BEGIN
        THROW 50010, 'RunId does not exist in ctl.IngestionRun.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
          AND Status <> 'Started'
    )
    BEGIN
        THROW 50011, 'Only runs with Status = Started can be completed.', 1;
    END;

    IF @RowsRead IS NOT NULL AND @RowsRead < 0
    BEGIN
        THROW 50012, '@RowsRead cannot be negative.', 1;
    END;

    IF @RowsCopied IS NOT NULL AND @RowsCopied < 0
    BEGIN
        THROW 50013, '@RowsCopied cannot be negative.', 1;
    END;

    /*
    ===========================================================================
    Complete ingestion run
    ===========================================================================
    */

    UPDATE ctl.IngestionRun
    SET
        Status = 'Succeeded',
        RowsRead = @RowsRead,
        RowsCopied = @RowsCopied,
        NewWatermarkValue = @NewWatermarkValue,
        DestinationContainer = COALESCE(@DestinationContainer, DestinationContainer),
        DestinationFolder = COALESCE(@DestinationFolder, DestinationFolder),
        EndedAt = @EndedAt,
        DurationSeconds = DATEDIFF(SECOND, StartedAt, @EndedAt),
        ErrorMessage = NULL
    WHERE RunId = @RunId;

    /*
    ===========================================================================
    Return compact result for ADF / evidence
    ===========================================================================
    */

    SELECT
        RunId,
        PipelineRunId,
        SourceObjectName,
        SourceType,
        Status,
        OldWatermarkValue,
        CurrentHighWatermarkValue,
        NewWatermarkValue,
        RowsRead,
        RowsCopied,
        DestinationContainer,
        DestinationFolder,
        StartedAt,
        EndedAt,
        DurationSeconds
    FROM ctl.IngestionRun
    WHERE RunId = @RunId;
END;
GO

/*
===============================================================================
Validation — Complete ingestion run for Orders
This validation uses a transaction and rolls back, so it does not leave test
records in ctl.IngestionRun.
===============================================================================
*/

DECLARE
    @OrdersSourceObjectId int,
    @OldWatermarkValue datetime2(3),
    @CurrentHighWatermarkValue datetime2(3),
    @RunId uniqueidentifier;

SELECT
    @OrdersSourceObjectId = SourceObjectId,
    @OldWatermarkValue = LastWatermarkValue
FROM ctl.SourceObject
WHERE SourceSystemName = 'sales_local'
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = 'dbo'
  AND SourceObjectName = 'Orders';

SELECT
    @CurrentHighWatermarkValue = MAX(UpdatedAt)
FROM dbo.Orders;

DECLARE @StartResult TABLE
(
    RunId uniqueidentifier,
    PipelineRunId varchar(100),
    SourceObjectId int,
    SourceSystemName varchar(100),
    SourceObjectName sysname,
    SourceType varchar(30),
    Status varchar(30)
);

BEGIN TRANSACTION;

INSERT INTO @StartResult
EXEC ctl.usp_StartIngestionRun
    @SourceObjectId = @OrdersSourceObjectId,
    @PipelineRunId = 'manual-validation-complete-run',
    @OldWatermarkValue = @OldWatermarkValue,
    @CurrentHighWatermarkValue = @CurrentHighWatermarkValue;

SELECT
    @RunId = RunId
FROM @StartResult;

EXEC ctl.usp_CompleteIngestionRun
    @RunId = @RunId,
    @RowsRead = 50,
    @RowsCopied = 50,
    @NewWatermarkValue = @CurrentHighWatermarkValue,
    @DestinationContainer = 'bronze',
    @DestinationFolder = 'sqlserver/sales_local/dbo/orders/load_date=2026-05-11/run_id=manual-validation-complete-run';

SELECT
    PipelineRunId,
    SourceObjectName,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    NewWatermarkValue,
    RowsRead,
    RowsCopied,
    DestinationFolder,
    DurationSeconds
FROM ctl.IngestionRun
WHERE RunId = @RunId;

ROLLBACK TRANSACTION;
GO