/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         07_usp_UpdateWatermark.sql
Purpose:        Update the source object watermark after a successful ingestion
                run and insert a watermark history record.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_UpdateWatermark

Used by:
- PL_01_SQL_Incremental_Ingestion

Notes:
- This procedure must only be called after successful copy.
- It updates ctl.SourceObject.LastWatermarkValue.
- It inserts one record into ctl.WatermarkHistory.
- Both operations are executed in the same transaction.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_UpdateWatermark
(
    @SourceObjectId          int,
    @RunId                   uniqueidentifier,
    @PreviousWatermarkValue  datetime2(3),
    @NewWatermarkValue       datetime2(3),
    @PipelineRunId           varchar(100) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    /*
    ===========================================================================
    Validate inputs
    ===========================================================================
    */

    IF @SourceObjectId IS NULL
    BEGIN
        THROW 50017, '@SourceObjectId cannot be NULL.', 1;
    END;

    IF @RunId IS NULL
    BEGIN
        THROW 50018, '@RunId cannot be NULL.', 1;
    END;

    IF @PreviousWatermarkValue IS NULL
    BEGIN
        THROW 50019, '@PreviousWatermarkValue cannot be NULL.', 1;
    END;

    IF @NewWatermarkValue IS NULL
    BEGIN
        THROW 50020, '@NewWatermarkValue cannot be NULL.', 1;
    END;

    IF @NewWatermarkValue < @PreviousWatermarkValue
    BEGIN
        THROW 50021, '@NewWatermarkValue cannot be lower than @PreviousWatermarkValue.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.SourceObject
        WHERE SourceObjectId = @SourceObjectId
          AND SourceType = 'SQL_TABLE'
    )
    BEGIN
        THROW 50022, 'SourceObjectId does not exist or is not a SQL_TABLE source.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
          AND SourceObjectId = @SourceObjectId
    )
    BEGIN
        THROW 50023, 'RunId does not exist for the provided SourceObjectId.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
          AND SourceObjectId = @SourceObjectId
          AND Status = 'Succeeded'
    )
    BEGIN
        THROW 50024, 'Watermark can only be updated for runs with Status = Succeeded.', 1;
    END;

    /*
    ===========================================================================
    Update watermark and insert history
    ===========================================================================
    */

    BEGIN TRANSACTION;

        UPDATE ctl.SourceObject
        SET
            LastWatermarkValue = @NewWatermarkValue,
            UpdatedAt = SYSUTCDATETIME()
        WHERE SourceObjectId = @SourceObjectId;

        INSERT INTO ctl.WatermarkHistory
        (
            SourceObjectId,
            RunId,
            PreviousWatermarkValue,
            NewWatermarkValue,
            AppliedAt,
            AppliedByPipelineRunId
        )
        VALUES
        (
            @SourceObjectId,
            @RunId,
            @PreviousWatermarkValue,
            @NewWatermarkValue,
            SYSUTCDATETIME(),
            @PipelineRunId
        );

    COMMIT TRANSACTION;

    /*
    ===========================================================================
    Return compact result for ADF / evidence
    ===========================================================================
    */

    SELECT
        s.SourceObjectId,
        s.SourceSystemName,
        s.SourceObjectName,
        s.WatermarkColumn,
        @PreviousWatermarkValue AS PreviousWatermarkValue,
        s.LastWatermarkValue AS CurrentLastWatermarkValue,
        wh.NewWatermarkValue,
        wh.AppliedByPipelineRunId,
        wh.AppliedAt
    FROM ctl.SourceObject AS s
    INNER JOIN ctl.WatermarkHistory AS wh
        ON s.SourceObjectId = wh.SourceObjectId
    WHERE wh.RunId = @RunId
      AND s.SourceObjectId = @SourceObjectId;
END;
GO

/*
===============================================================================
Validation — Update watermark for Orders
This validation uses a transaction and rolls back, so it does not leave test
records in ctl.IngestionRun or ctl.WatermarkHistory, and it does not permanently
change ctl.SourceObject.LastWatermarkValue.
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
    @PipelineRunId = 'manual-validation-update-watermark',
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
    @DestinationFolder = 'sqlserver/sales_local/dbo/orders/load_date=2026-05-11/run_id=manual-validation-update-watermark';

EXEC ctl.usp_UpdateWatermark
    @SourceObjectId = @OrdersSourceObjectId,
    @RunId = @RunId,
    @PreviousWatermarkValue = @OldWatermarkValue,
    @NewWatermarkValue = @CurrentHighWatermarkValue,
    @PipelineRunId = 'manual-validation-update-watermark';

SELECT
    s.SourceObjectName,
    s.WatermarkColumn,
    s.LastWatermarkValue,
    wh.PreviousWatermarkValue,
    wh.NewWatermarkValue,
    wh.AppliedByPipelineRunId
FROM ctl.SourceObject AS s
INNER JOIN ctl.WatermarkHistory AS wh
    ON s.SourceObjectId = wh.SourceObjectId
WHERE wh.RunId = @RunId;

ROLLBACK TRANSACTION;
GO