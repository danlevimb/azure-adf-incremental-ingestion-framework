/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         04_usp_StartIngestionRun.sql
Purpose:        Start an ingestion run record for a source object before data
                copy begins.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_StartIngestionRun

Used by:
- PL_01_SQL_Incremental_Ingestion
- PL_02_File_Ingestion

Notes:
- This procedure creates a ctl.IngestionRun row with Status = 'Started'.
- It denormalizes source metadata for easier evidence and troubleshooting.
- It returns the generated RunId to be used by later logging procedures.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_StartIngestionRun
(
    @SourceObjectId            int,
    @PipelineRunId             varchar(100) = NULL,
    @OldWatermarkValue         datetime2(3) = NULL,
    @CurrentHighWatermarkValue datetime2(3) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @RunId                uniqueidentifier = NEWID(),
        @SourceSystemName     varchar(100),
        @SourceType           varchar(30),
        @SourceObjectName     sysname,
        @DestinationContainer varchar(100),
        @DestinationFolder    varchar(500);

    /*
    ===========================================================================
    Validate input
    ===========================================================================
    */

    IF @SourceObjectId IS NULL
    BEGIN
        THROW 50007, '@SourceObjectId cannot be NULL.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.SourceObject
        WHERE SourceObjectId = @SourceObjectId
    )
    BEGIN
        THROW 50008, 'SourceObjectId does not exist in ctl.SourceObject.', 1;
    END;

    /*
    ===========================================================================
    Read source metadata
    ===========================================================================
    */

    SELECT
        @SourceSystemName = SourceSystemName,
        @SourceType = SourceType,
        @SourceObjectName = SourceObjectName,
        @DestinationContainer = DestinationContainer,
        @DestinationFolder = DestinationFolder
    FROM ctl.SourceObject
    WHERE SourceObjectId = @SourceObjectId;

    /*
    ===========================================================================
    Insert Started run
    ===========================================================================
    */

    INSERT INTO ctl.IngestionRun
    (
        RunId,
        PipelineRunId,
        SourceObjectId,
        SourceSystemName,
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
        DurationSeconds,
        ErrorMessage
    )
    VALUES
    (
        @RunId,
        @PipelineRunId,
        @SourceObjectId,
        @SourceSystemName,
        @SourceObjectName,
        @SourceType,
        'Started',
        @OldWatermarkValue,
        @CurrentHighWatermarkValue,
        NULL,
        NULL,
        NULL,
        @DestinationContainer,
        @DestinationFolder,
        SYSUTCDATETIME(),
        NULL,
        NULL,
        NULL
    );

    /*
    ===========================================================================
    Return RunId for ADF
    ===========================================================================
    */

    SELECT
        @RunId AS RunId,
        @PipelineRunId AS PipelineRunId,
        @SourceObjectId AS SourceObjectId,
        @SourceSystemName AS SourceSystemName,
        @SourceObjectName AS SourceObjectName,
        @SourceType AS SourceType,
        'Started' AS Status;
END;
GO

/*
===============================================================================
Validation — Start ingestion run for Orders
This validation uses a transaction and rolls back, so it does not leave test
records in ctl.IngestionRun.
===============================================================================
*/

DECLARE
    @OrdersSourceObjectId int,
    @OldWatermarkValue datetime2(3),
    @CurrentHighWatermarkValue datetime2(3);

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

BEGIN TRANSACTION;

EXEC ctl.usp_StartIngestionRun
    @SourceObjectId = @OrdersSourceObjectId,
    @PipelineRunId = 'manual-validation-start-run',
    @OldWatermarkValue = @OldWatermarkValue,
    @CurrentHighWatermarkValue = @CurrentHighWatermarkValue;

SELECT
    RunId,
    PipelineRunId,
    SourceObjectName,
    SourceType,
    Status,
    OldWatermarkValue,
    CurrentHighWatermarkValue,
    DestinationContainer,
    DestinationFolder,
    StartedAt
FROM ctl.IngestionRun
WHERE PipelineRunId = 'manual-validation-start-run';

ROLLBACK TRANSACTION;
GO