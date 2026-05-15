/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         06_failed_run_setup.sql
Purpose:        Prepare or restore a controlled failure scenario for ADF
                incremental ingestion testing.

Database:       ADF_Ingestion_Source
Phase:          Test Scenario Preparation

Scenario:
- Failed run setup

IMPORTANT:
- Default mode is PREVIEW.
- Do NOT use PREPARE_FAILURE until ADF pipelines are implemented and a
  successful initial full load has already been completed.
- This script is designed to temporarily misconfigure the ADLS destination
  container for one source object so the Copy Activity can fail after the
  ingestion run has started.
- After the failure evidence is captured, run RESTORE_CONFIGURATION.

Supported actions:
- PREVIEW
- PREPARE_FAILURE
- RESTORE_CONFIGURATION

Target:
- dbo.Orders source object

Expected failure behavior:
- ADF starts the ingestion run.
- Copy Activity fails due to invalid destination container.
- ctl.usp_FailIngestionRun marks the run as Failed.
- ctl.SourceObject.LastWatermarkValue remains unchanged.
- ctl.WatermarkHistory does not receive a new record.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

SET NOCOUNT ON;
GO

DECLARE @Action varchar(30) = 'PREVIEW';
-- Allowed values:
-- PREVIEW
-- PREPARE_FAILURE
-- RESTORE_CONFIGURATION

DECLARE
    @SourceSystemName varchar(100) = 'sales_local',
    @SourceSchema sysname = 'dbo',
    @SourceObjectName sysname = 'Orders',
    @ExpectedContainer varchar(100) = 'bronze',
    @ExpectedFolder varchar(500) = 'sqlserver/sales_local/dbo/orders',
    @FailureContainer varchar(100) = 'bronze_invalid_for_failure_test',
    @SourceObjectId int;

SELECT
    @SourceObjectId = SourceObjectId
FROM ctl.SourceObject
WHERE SourceSystemName = @SourceSystemName
  AND SourceType = 'SQL_TABLE'
  AND SourceSchema = @SourceSchema
  AND SourceObjectName = @SourceObjectName;

IF @SourceObjectId IS NULL
BEGIN
    THROW 54001, 'Target SourceObject was not found.', 1;
END;

IF @Action NOT IN ('PREVIEW', 'PREPARE_FAILURE', 'RESTORE_CONFIGURATION')
BEGIN
    THROW 54002, 'Invalid @Action. Expected PREVIEW, PREPARE_FAILURE, or RESTORE_CONFIGURATION.', 1;
END;

/*
===============================================================================
1. Current configuration preview
===============================================================================
*/

SELECT
    'CURRENT_CONFIGURATION' AS ResultType,
    SourceObjectId,
    SourceObjectName,
    LastWatermarkValue,
    DestinationContainer,
    DestinationFolder,
    DestinationFormat
FROM ctl.SourceObject
WHERE SourceObjectId = @SourceObjectId;

/*
===============================================================================
2. Preview mode
===============================================================================
*/

IF @Action = 'PREVIEW'
BEGIN
    PRINT 'PREVIEW MODE ONLY. No configuration changes will be applied.';

    SELECT
        'PREVIEW_ONLY' AS Mode,
        @SourceObjectName AS TargetSourceObject,
        @FailureContainer AS FailureDestinationContainer,
        'When @Action = PREPARE_FAILURE, DestinationContainer will be changed temporarily to force an ADF Copy Activity failure.' AS Description;

    SELECT
        'RESTORE_COMMAND_NOTE' AS NoteType,
        'After failure evidence is captured, run this script with @Action = RESTORE_CONFIGURATION.' AS Note;

    RETURN;
END;

/*
===============================================================================
3. Prepare controlled failure
===============================================================================
*/

IF @Action = 'PREPARE_FAILURE'
BEGIN
    IF NOT EXISTS
    (
        SELECT 1
        FROM ctl.SourceObject
        WHERE SourceObjectId = @SourceObjectId
          AND LastWatermarkValue > CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
    )
    BEGIN
        THROW 54003, 'Initial full load does not appear to be completed. Do not prepare failure before watermarks have advanced.', 1;
    END;

    UPDATE ctl.SourceObject
    SET
        DestinationContainer = @FailureContainer,
        UpdatedAt = SYSUTCDATETIME()
    WHERE SourceObjectId = @SourceObjectId;

    PRINT 'Controlled failure configuration prepared.';

    SELECT
        'FAILURE_CONFIGURATION_PREPARED' AS ResultType,
        SourceObjectId,
        SourceObjectName,
        LastWatermarkValue,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    FROM ctl.SourceObject
    WHERE SourceObjectId = @SourceObjectId;

    RETURN;
END;

/*
===============================================================================
4. Restore original configuration
===============================================================================
*/

IF @Action = 'RESTORE_CONFIGURATION'
BEGIN
    UPDATE ctl.SourceObject
    SET
        DestinationContainer = @ExpectedContainer,
        DestinationFolder = @ExpectedFolder,
        UpdatedAt = SYSUTCDATETIME()
    WHERE SourceObjectId = @SourceObjectId;

    PRINT 'Original configuration restored.';

    SELECT
        'CONFIGURATION_RESTORED' AS ResultType,
        SourceObjectId,
        SourceObjectName,
        LastWatermarkValue,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    FROM ctl.SourceObject
    WHERE SourceObjectId = @SourceObjectId;

    RETURN;
END;
GO