/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         05_create_indexes.sql
Purpose:        Create supporting indexes for source and control metadata tables
                used by the ADF incremental ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Notes:
- This script is intentionally non-destructive.
- It creates indexes only if they do not already exist.
- Source table UpdatedAt indexes were created in 02_create_source_tables.sql.
- This script focuses on control metadata indexes.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
1. ctl.SourceObject indexes
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_SourceObject_IsActive_SourceType'
      AND object_id = OBJECT_ID(N'ctl.SourceObject')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_SourceObject_IsActive_SourceType
    ON ctl.SourceObject (IsActive, SourceType)
    INCLUDE
    (
        SourceObjectId,
        SourceSystemName,
        SourceSchema,
        SourceObjectName,
        LoadType,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    );

    PRINT 'Index [IX_SourceObject_IsActive_SourceType] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_SourceObject_IsActive_SourceType] already exists. No action taken.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_SourceObject_SourceSystem_Object'
      AND object_id = OBJECT_ID(N'ctl.SourceObject')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_SourceObject_SourceSystem_Object
    ON ctl.SourceObject
    (
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName
    );

    PRINT 'Index [IX_SourceObject_SourceSystem_Object] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_SourceObject_SourceSystem_Object] already exists. No action taken.';
END;
GO

/*
===============================================================================
2. ctl.FileSourceConfig indexes
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_FileSourceConfig_SourceObjectId'
      AND object_id = OBJECT_ID(N'ctl.FileSourceConfig')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_FileSourceConfig_SourceObjectId
    ON ctl.FileSourceConfig (SourceObjectId)
    INCLUDE
    (
        FileFormat,
        SourcePath,
        FileNamePattern,
        HasHeader,
        Delimiter,
        IsActive,
        DestinationFolder
    );

    PRINT 'Index [IX_FileSourceConfig_SourceObjectId] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_FileSourceConfig_SourceObjectId] already exists. No action taken.';
END;
GO

/*
===============================================================================
3. ctl.IngestionRun indexes
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_IngestionRun_SourceObjectId_StartedAt'
      AND object_id = OBJECT_ID(N'ctl.IngestionRun')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_IngestionRun_SourceObjectId_StartedAt
    ON ctl.IngestionRun
    (
        SourceObjectId,
        StartedAt
    )
    INCLUDE
    (
        Status,
        PipelineRunId,
        OldWatermarkValue,
        CurrentHighWatermarkValue,
        NewWatermarkValue,
        RowsRead,
        RowsCopied
    );

    PRINT 'Index [IX_IngestionRun_SourceObjectId_StartedAt] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_IngestionRun_SourceObjectId_StartedAt] already exists. No action taken.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_IngestionRun_PipelineRunId'
      AND object_id = OBJECT_ID(N'ctl.IngestionRun')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_IngestionRun_PipelineRunId
    ON ctl.IngestionRun (PipelineRunId);

    PRINT 'Index [IX_IngestionRun_PipelineRunId] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_IngestionRun_PipelineRunId] already exists. No action taken.';
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_IngestionRun_Status'
      AND object_id = OBJECT_ID(N'ctl.IngestionRun')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_IngestionRun_Status
    ON ctl.IngestionRun (Status, StartedAt)
    INCLUDE
    (
        SourceObjectId,
        SourceSystemName,
        SourceObjectName,
        SourceType
    );

    PRINT 'Index [IX_IngestionRun_Status] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_IngestionRun_Status] already exists. No action taken.';
END;
GO

/*
===============================================================================
4. ctl.IngestionRunStep indexes
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_IngestionRunStep_RunId'
      AND object_id = OBJECT_ID(N'ctl.IngestionRunStep')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_IngestionRunStep_RunId
    ON ctl.IngestionRunStep (RunId)
    INCLUDE
    (
        StepName,
        ActivityName,
        Status,
        StartedAt,
        EndedAt,
        RowsRead,
        RowsCopied
    );

    PRINT 'Index [IX_IngestionRunStep_RunId] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_IngestionRunStep_RunId] already exists. No action taken.';
END;
GO

/*
===============================================================================
5. ctl.WatermarkHistory indexes
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_WatermarkHistory_SourceObjectId_AppliedAt'
      AND object_id = OBJECT_ID(N'ctl.WatermarkHistory')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_WatermarkHistory_SourceObjectId_AppliedAt
    ON ctl.WatermarkHistory
    (
        SourceObjectId,
        AppliedAt
    )
    INCLUDE
    (
        RunId,
        PreviousWatermarkValue,
        NewWatermarkValue,
        AppliedByPipelineRunId
    );

    PRINT 'Index [IX_WatermarkHistory_SourceObjectId_AppliedAt] created successfully.';
END
ELSE
BEGIN
    PRINT 'Index [IX_WatermarkHistory_SourceObjectId_AppliedAt] already exists. No action taken.';
END;
GO

/*
===============================================================================
6. Validation — Control metadata indexes
===============================================================================
*/

SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc,
    i.is_unique
FROM sys.indexes AS i
WHERE OBJECT_SCHEMA_NAME(i.object_id) = N'ctl'
  AND i.name IN
  (
      N'IX_SourceObject_IsActive_SourceType',
      N'IX_SourceObject_SourceSystem_Object',
      N'IX_FileSourceConfig_SourceObjectId',
      N'IX_IngestionRun_SourceObjectId_StartedAt',
      N'IX_IngestionRun_PipelineRunId',
      N'IX_IngestionRun_Status',
      N'IX_IngestionRunStep_RunId',
      N'IX_WatermarkHistory_SourceObjectId_AppliedAt'
  )
ORDER BY
    TableName,
    IndexName;
GO

/*
===============================================================================
7. Validation — Index columns
===============================================================================
*/

SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    ic.key_ordinal,
    c.name AS ColumnName,
    ic.is_included_column
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic
    ON i.object_id = ic.object_id
    AND i.index_id = ic.index_id
INNER JOIN sys.columns AS c
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = N'ctl'
  AND i.name IN
  (
      N'IX_SourceObject_IsActive_SourceType',
      N'IX_SourceObject_SourceSystem_Object',
      N'IX_FileSourceConfig_SourceObjectId',
      N'IX_IngestionRun_SourceObjectId_StartedAt',
      N'IX_IngestionRun_PipelineRunId',
      N'IX_IngestionRun_Status',
      N'IX_IngestionRunStep_RunId',
      N'IX_WatermarkHistory_SourceObjectId_AppliedAt'
  )
ORDER BY
    TableName,
    IndexName,
    ic.key_ordinal,
    ic.index_column_id;
GO