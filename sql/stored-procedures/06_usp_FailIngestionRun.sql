/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         06_usp_FailIngestionRun.sql
Purpose:        Mark an ingestion run as failed when a pipeline or copy
                operation fails.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_FailIngestionRun

Used by:
- PL_01_SQL_Incremental_Ingestion
- PL_02_File_Ingestion

Notes:
- This procedure updates ctl.IngestionRun with Status = 'Failed'.
- It stores the failure message and execution duration.
- It does not update ctl.SourceObject.LastWatermarkValue.
- It does not insert into ctl.WatermarkHistory.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_FailIngestionRun
(
    @RunId        uniqueidentifier,
    @ErrorMessage nvarchar(max)
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
        THROW 50014, '@RunId cannot be NULL.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
    )
    BEGIN
        THROW 50015, 'RunId does not exist in ctl.IngestionRun.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
          AND Status <> 'Started'
    )
    BEGIN
        THROW 50016, 'Only runs with Status = Started can be marked as Failed.', 1;
    END;

    /*
    ===========================================================================
    Fail ingestion run
    ===========================================================================
    */

    UPDATE ctl.IngestionRun
    SET
        Status = 'Failed',
        EndedAt = @EndedAt,
        DurationSeconds = DATEDIFF(SECOND, StartedAt, @EndedAt),
        ErrorMessage = @ErrorMessage,
        NewWatermarkValue = NULL,
        RowsRead = COALESCE(RowsRead, 0),
        RowsCopied = COALESCE(RowsCopied, 0)
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
        ErrorMessage,
        StartedAt,
        EndedAt,
        DurationSeconds
    FROM ctl.IngestionRun
    WHERE RunId = @RunId;
END;
GO

/*
===============================================================================
Validation — Fail ingestion run for Orders
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
    @PipelineRunId = 'manual-validation-failed-run',
    @OldWatermarkValue = @OldWatermarkValue,
    @CurrentHighWatermarkValue = @CurrentHighWatermarkValue;

SELECT
    @RunId = RunId
FROM @StartResult;

EXEC ctl.usp_FailIngestionRun
    @RunId = @RunId,
    @ErrorMessage = 'Manual validation failure. Simulated Copy Activity error.';

SELECT
    PipelineRunId,
    SourceObjectName,
    SourceType,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    NewWatermarkValue,
    RowsRead,
    RowsCopied,
    ErrorMessage,
    DurationSeconds
FROM ctl.IngestionRun
WHERE RunId = @RunId;

ROLLBACK TRANSACTION;
GO