/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         08_usp_LogIngestionRunStep.sql
Purpose:        Insert step-level execution logs for ingestion runs.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_LogIngestionRunStep

Used by:
- PL_01_SQL_Incremental_Ingestion
- PL_02_File_Ingestion

Notes:
- This procedure writes records into ctl.IngestionRunStep.
- Step logging is optional/lightweight for the MVP, but useful for evidence,
  troubleshooting, and operational traceability.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_LogIngestionRunStep
(
    @RunId        uniqueidentifier,
    @StepName     varchar(100),
    @ActivityName varchar(150) = NULL,
    @Status       varchar(30),
    @RowsRead     bigint = NULL,
    @RowsCopied   bigint = NULL,
    @StartedAt    datetime2(3) = NULL,
    @EndedAt      datetime2(3) = NULL,
    @ErrorMessage nvarchar(max) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @EffectiveStartedAt datetime2(3) = COALESCE(@StartedAt, SYSUTCDATETIME()),
        @EffectiveEndedAt   datetime2(3) = @EndedAt,
        @DurationSeconds    int = NULL;

    /*
    ===========================================================================
    Validate inputs
    ===========================================================================
    */

    IF @RunId IS NULL
    BEGIN
        THROW 50025, '@RunId cannot be NULL.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.IngestionRun
        WHERE RunId = @RunId
    )
    BEGIN
        THROW 50026, 'RunId does not exist in ctl.IngestionRun.', 1;
    END;

    IF @StepName IS NULL OR LTRIM(RTRIM(@StepName)) = ''
    BEGIN
        THROW 50027, '@StepName cannot be NULL or empty.', 1;
    END;

    IF @Status NOT IN ('Started', 'Succeeded', 'Failed', 'Skipped')
    BEGIN
        THROW 50028, 'Invalid @Status. Expected values: Started, Succeeded, Failed, Skipped.', 1;
    END;

    IF @RowsRead IS NOT NULL AND @RowsRead < 0
    BEGIN
        THROW 50029, '@RowsRead cannot be negative.', 1;
    END;

    IF @RowsCopied IS NOT NULL AND @RowsCopied < 0
    BEGIN
        THROW 50030, '@RowsCopied cannot be negative.', 1;
    END;

    IF @EffectiveEndedAt IS NOT NULL
    BEGIN
        SET @DurationSeconds = DATEDIFF(SECOND, @EffectiveStartedAt, @EffectiveEndedAt);
    END;

    /*
    ===========================================================================
    Insert step log
    ===========================================================================
    */

    INSERT INTO ctl.IngestionRunStep
    (
        RunId,
        StepName,
        ActivityName,
        Status,
        RowsRead,
        RowsCopied,
        StartedAt,
        EndedAt,
        DurationSeconds,
        ErrorMessage
    )
    VALUES
    (
        @RunId,
        @StepName,
        @ActivityName,
        @Status,
        @RowsRead,
        @RowsCopied,
        @EffectiveStartedAt,
        @EffectiveEndedAt,
        @DurationSeconds,
        @ErrorMessage
    );

    /*
    ===========================================================================
    Return compact result for ADF / evidence
    ===========================================================================
    */

    SELECT
        RunStepId,
        RunId,
        StepName,
        ActivityName,
        Status,
        RowsRead,
        RowsCopied,
        StartedAt,
        EndedAt,
        DurationSeconds,
        ErrorMessage
    FROM ctl.IngestionRunStep
    WHERE RunStepId = SCOPE_IDENTITY();
END;
GO

/*
===============================================================================
Validation — Log ingestion run steps for Orders
This validation uses a transaction and rolls back, so it does not leave test
records in ctl.IngestionRun or ctl.IngestionRunStep.
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
    @PipelineRunId = 'manual-validation-log-step',
    @OldWatermarkValue = @OldWatermarkValue,
    @CurrentHighWatermarkValue = @CurrentHighWatermarkValue;

SELECT
    @RunId = RunId
FROM @StartResult;

EXEC ctl.usp_LogIngestionRunStep
    @RunId = @RunId,
    @StepName = 'Get Source Configuration',
    @ActivityName = 'ACT_Lookup_Source_Config',
    @Status = 'Succeeded',
    @RowsRead = NULL,
    @RowsCopied = NULL,
    @StartedAt = '2026-05-11T19:00:00.000',
    @EndedAt = '2026-05-11T19:00:05.000',
    @ErrorMessage = NULL;

EXEC ctl.usp_LogIngestionRunStep
    @RunId = @RunId,
    @StepName = 'Copy SQL To ADLS',
    @ActivityName = 'ACT_Copy_SQL_To_ADLS',
    @Status = 'Succeeded',
    @RowsRead = 50,
    @RowsCopied = 50,
    @StartedAt = '2026-05-11T19:00:06.000',
    @EndedAt = '2026-05-11T19:00:20.000',
    @ErrorMessage = NULL;

SELECT
    StepName,
    ActivityName,
    Status,
    RowsRead,
    RowsCopied,
    StartedAt,
    EndedAt,
    DurationSeconds
FROM ctl.IngestionRunStep
WHERE RunId = @RunId
ORDER BY
    RunStepId;

ROLLBACK TRANSACTION;
GO