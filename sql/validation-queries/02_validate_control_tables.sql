/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         02_validate_control_tables.sql
Purpose:        Validate the control metadata layer after creating control
                schema, control tables, indexes, seed metadata, and stored
                procedures.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Validation focus:
- Control tables exist
- Control indexes exist
- SourceObject seed data exists
- FileSourceConfig seed data exists
- Initial SQL watermarks are configured
- Stored procedures exist
- Key metadata counts return expected values
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
Validation 1 — Control tables exist
Compact PASS / FAIL output
===============================================================================
*/

WITH ExpectedTables AS
(
    SELECT 'SourceObject' AS TableName
    UNION ALL SELECT 'FileSourceConfig'
    UNION ALL SELECT 'IngestionRun'
    UNION ALL SELECT 'IngestionRunStep'
    UNION ALL SELECT 'WatermarkHistory'
),
ActualTables AS
(
    SELECT
        t.name AS TableName
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s
        ON t.schema_id = s.schema_id
    WHERE s.name = 'ctl'
)
SELECT
    e.TableName,
    CASE
        WHEN a.TableName IS NOT NULL THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ExpectedTables AS e
LEFT JOIN ActualTables AS a
    ON e.TableName = a.TableName
ORDER BY
    e.TableName;
GO

/*
===============================================================================
Validation 2 — Control indexes exist
Compact PASS / FAIL output
===============================================================================
*/

WITH ExpectedIndexes AS
(
    SELECT 'IX_SourceObject_IsActive_SourceType' AS IndexName
    UNION ALL SELECT 'IX_SourceObject_SourceSystem_Object'
    UNION ALL SELECT 'IX_FileSourceConfig_SourceObjectId'
    UNION ALL SELECT 'IX_IngestionRun_SourceObjectId_StartedAt'
    UNION ALL SELECT 'IX_IngestionRun_PipelineRunId'
    UNION ALL SELECT 'IX_IngestionRun_Status'
    UNION ALL SELECT 'IX_IngestionRunStep_RunId'
    UNION ALL SELECT 'IX_WatermarkHistory_SourceObjectId_AppliedAt'
),
ActualIndexes AS
(
    SELECT
        i.name AS IndexName
    FROM sys.indexes AS i
    WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'ctl'
)
SELECT
    e.IndexName,
    CASE
        WHEN a.IndexName IS NOT NULL THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ExpectedIndexes AS e
LEFT JOIN ActualIndexes AS a
    ON e.IndexName = a.IndexName
ORDER BY
    e.IndexName;
GO

/*
===============================================================================
Validation 3 — Source object counts by type
Expected:
- CSV_FILE  = 2
- JSON_FILE = 2
- SQL_TABLE = 5
===============================================================================
*/

WITH ExpectedCounts AS
(
    SELECT 'CSV_FILE' AS SourceType, 2 AS Expected_Count
    UNION ALL SELECT 'JSON_FILE', 2
    UNION ALL SELECT 'SQL_TABLE', 5
),
ActualCounts AS
(
    SELECT
        SourceType,
        COUNT(*) AS Actual_Count
    FROM ctl.SourceObject
    GROUP BY
        SourceType
)
SELECT
    e.SourceType,
    e.Expected_Count,
    ISNULL(a.Actual_Count, 0) AS Actual_Count,
    CASE
        WHEN e.Expected_Count = ISNULL(a.Actual_Count, 0) THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ExpectedCounts AS e
LEFT JOIN ActualCounts AS a
    ON e.SourceType = a.SourceType
ORDER BY
    e.SourceType;
GO

/*
===============================================================================
Validation 4 — SQL source initial watermark setup
Expected:
- 5 active SQL_TABLE source objects
- WatermarkColumn = UpdatedAt
- LastWatermarkValue = 1900-01-01 00:00:00.000
- LoadType = INCREMENTAL
- DestinationFormat = PARQUET
===============================================================================
*/

SELECT
    SourceObjectName,
    WatermarkColumn,
    LastWatermarkValue,
    LoadType,
    DestinationFormat,
    CASE
        WHEN WatermarkColumn = 'UpdatedAt'
         AND LastWatermarkValue = CONVERT(datetime2(3), '1900-01-01T00:00:00.000')
         AND LoadType = 'INCREMENTAL'
         AND DestinationFormat = 'PARQUET'
        THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ctl.SourceObject
WHERE SourceType = 'SQL_TABLE'
ORDER BY
    SourceObjectName;
GO

/*
===============================================================================
Validation 5 — File source configuration exists
Expected:
- 4 active file source configs
===============================================================================
*/

SELECT
    s.SourceObjectName,
    s.SourceType,
    f.FileFormat,
    f.SourcePath,
    f.FileNamePattern,
    CASE
        WHEN f.IsActive = 1 THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ctl.SourceObject AS s
INNER JOIN ctl.FileSourceConfig AS f
    ON s.SourceObjectId = f.SourceObjectId
WHERE s.SourceType IN ('CSV_FILE', 'JSON_FILE')
ORDER BY
    s.SourceType,
    s.SourceObjectName;
GO

/*
===============================================================================
Validation 6 — Stored procedures exist
===============================================================================
*/

WITH ExpectedProcedures AS
(
    SELECT 'usp_GetActiveSourceObjects' AS ProcedureName
    UNION ALL SELECT 'usp_GetSourceObjectConfig'
    UNION ALL SELECT 'usp_GetCurrentHighWatermark'
    UNION ALL SELECT 'usp_StartIngestionRun'
    UNION ALL SELECT 'usp_CompleteIngestionRun'
    UNION ALL SELECT 'usp_FailIngestionRun'
    UNION ALL SELECT 'usp_UpdateWatermark'
    UNION ALL SELECT 'usp_LogIngestionRunStep'
),
ActualProcedures AS
(
    SELECT
        p.name AS ProcedureName
    FROM sys.procedures AS p
    INNER JOIN sys.schemas AS s
        ON p.schema_id = s.schema_id
    WHERE s.name = 'ctl'
)
SELECT
    e.ProcedureName,
    CASE
        WHEN a.ProcedureName IS NOT NULL THEN 'PASS'
        ELSE 'FAIL'
    END AS Validation_Status
FROM ExpectedProcedures AS e
LEFT JOIN ActualProcedures AS a
    ON e.ProcedureName = a.ProcedureName
ORDER BY
    e.ProcedureName;
GO

/*
===============================================================================
Validation 7 — Control metadata layer summary
===============================================================================
*/

SELECT
    'Control Tables' AS ValidationArea,
    COUNT(*) AS Object_Count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
WHERE s.name = 'ctl'

UNION ALL

SELECT
    'Control Stored Procedures',
    COUNT(*)
FROM sys.procedures AS p
INNER JOIN sys.schemas AS s
    ON p.schema_id = s.schema_id
WHERE s.name = 'ctl'

UNION ALL

SELECT
    'Source Objects',
    COUNT(*)
FROM ctl.SourceObject

UNION ALL

SELECT
    'File Source Configs',
    COUNT(*)
FROM ctl.FileSourceConfig;
GO