/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         04_create_control_tables.sql
Purpose:        Create control metadata tables used by the ADF incremental
                ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Control tables:
- ctl.SourceObject
- ctl.FileSourceConfig
- ctl.IngestionRun
- ctl.IngestionRunStep
- ctl.WatermarkHistory

Notes:
- This script is intentionally non-destructive.
- It creates tables only if they do not already exist.
- Control tables drive metadata-based ingestion, execution logging,
  file ingestion configuration, and watermark history.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
1. ctl.SourceObject
===============================================================================
*/

IF OBJECT_ID(N'ctl.SourceObject', N'U') IS NULL
BEGIN
    PRINT 'Creating table [ctl].[SourceObject]...';

    CREATE TABLE ctl.SourceObject
    (
        SourceObjectId       int IDENTITY(1,1) NOT NULL,
        SourceSystemName     varchar(100) NOT NULL,
        SourceType           varchar(30) NOT NULL,
        SourceSchema         sysname NULL,
        SourceObjectName     sysname NOT NULL,
        WatermarkColumn      sysname NULL,
        LastWatermarkValue   datetime2(3) NULL,
        LoadType             varchar(30) NOT NULL,
        IsActive             bit NOT NULL
            CONSTRAINT DF_SourceObject_IsActive DEFAULT (1),
        DestinationContainer varchar(100) NOT NULL,
        DestinationFolder    varchar(500) NOT NULL,
        DestinationFormat    varchar(30) NOT NULL,
        CreatedAt            datetime2(3) NOT NULL
            CONSTRAINT DF_SourceObject_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt            datetime2(3) NOT NULL
            CONSTRAINT DF_SourceObject_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_SourceObject
            PRIMARY KEY CLUSTERED (SourceObjectId),

        CONSTRAINT CK_SourceObject_SourceType
            CHECK (SourceType IN ('SQL_TABLE', 'CSV_FILE', 'JSON_FILE')),

        CONSTRAINT CK_SourceObject_LoadType
            CHECK (LoadType IN ('FULL', 'INCREMENTAL')),

        CONSTRAINT CK_SourceObject_DestinationFormat
            CHECK (DestinationFormat IN ('PARQUET', 'CSV', 'JSON')),

        CONSTRAINT CK_SourceObject_SQL_Watermark
            CHECK
            (
                SourceType <> 'SQL_TABLE'
                OR
                (
                    SourceType = 'SQL_TABLE'
                    AND SourceSchema IS NOT NULL
                    AND WatermarkColumn IS NOT NULL
                )
            )
    );

    PRINT 'Table [ctl].[SourceObject] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [ctl].[SourceObject] already exists. No action taken.';
END;
GO

/*
===============================================================================
2. ctl.FileSourceConfig
===============================================================================
*/

IF OBJECT_ID(N'ctl.FileSourceConfig', N'U') IS NULL
BEGIN
    PRINT 'Creating table [ctl].[FileSourceConfig]...';

    CREATE TABLE ctl.FileSourceConfig
    (
        FileSourceConfigId int IDENTITY(1,1) NOT NULL,
        SourceObjectId     int NOT NULL,
        FileFormat         varchar(30) NOT NULL,
        SourcePath         varchar(500) NOT NULL,
        FileNamePattern    varchar(255) NOT NULL,
        HasHeader          bit NULL,
        Delimiter          varchar(10) NULL,
        IsActive           bit NOT NULL
            CONSTRAINT DF_FileSourceConfig_IsActive DEFAULT (1),
        DestinationFolder  varchar(500) NOT NULL,
        CreatedAt          datetime2(3) NOT NULL
            CONSTRAINT DF_FileSourceConfig_CreatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedAt          datetime2(3) NOT NULL
            CONSTRAINT DF_FileSourceConfig_UpdatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_FileSourceConfig
            PRIMARY KEY CLUSTERED (FileSourceConfigId),

        CONSTRAINT FK_FileSourceConfig_SourceObject
            FOREIGN KEY (SourceObjectId)
            REFERENCES ctl.SourceObject (SourceObjectId),

        CONSTRAINT CK_FileSourceConfig_FileFormat
            CHECK (FileFormat IN ('CSV', 'JSON'))
    );

    PRINT 'Table [ctl].[FileSourceConfig] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [ctl].[FileSourceConfig] already exists. No action taken.';
END;
GO

/*
===============================================================================
3. ctl.IngestionRun
===============================================================================
*/

IF OBJECT_ID(N'ctl.IngestionRun', N'U') IS NULL
BEGIN
    PRINT 'Creating table [ctl].[IngestionRun]...';

    CREATE TABLE ctl.IngestionRun
    (
        RunId                     uniqueidentifier NOT NULL
            CONSTRAINT DF_IngestionRun_RunId DEFAULT (NEWID()),
        PipelineRunId              varchar(100) NULL,
        SourceObjectId             int NOT NULL,
        SourceSystemName           varchar(100) NOT NULL,
        SourceObjectName           sysname NOT NULL,
        SourceType                 varchar(30) NOT NULL,
        Status                     varchar(30) NOT NULL,
        OldWatermarkValue          datetime2(3) NULL,
        CurrentHighWatermarkValue  datetime2(3) NULL,
        NewWatermarkValue          datetime2(3) NULL,
        RowsRead                   bigint NULL,
        RowsCopied                 bigint NULL,
        DestinationContainer       varchar(100) NULL,
        DestinationFolder          varchar(500) NULL,
        StartedAt                  datetime2(3) NOT NULL
            CONSTRAINT DF_IngestionRun_StartedAt DEFAULT (SYSUTCDATETIME()),
        EndedAt                    datetime2(3) NULL,
        DurationSeconds            int NULL,
        ErrorMessage               nvarchar(max) NULL,
        CreatedAt                  datetime2(3) NOT NULL
            CONSTRAINT DF_IngestionRun_CreatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_IngestionRun
            PRIMARY KEY CLUSTERED (RunId),

        CONSTRAINT FK_IngestionRun_SourceObject
            FOREIGN KEY (SourceObjectId)
            REFERENCES ctl.SourceObject (SourceObjectId),

        CONSTRAINT CK_IngestionRun_SourceType
            CHECK (SourceType IN ('SQL_TABLE', 'CSV_FILE', 'JSON_FILE')),

        CONSTRAINT CK_IngestionRun_Status
            CHECK (Status IN ('Started', 'Succeeded', 'Failed', 'Skipped')),

        CONSTRAINT CK_IngestionRun_RowsRead
            CHECK (RowsRead IS NULL OR RowsRead >= 0),

        CONSTRAINT CK_IngestionRun_RowsCopied
            CHECK (RowsCopied IS NULL OR RowsCopied >= 0),

        CONSTRAINT CK_IngestionRun_DurationSeconds
            CHECK (DurationSeconds IS NULL OR DurationSeconds >= 0)
    );

    PRINT 'Table [ctl].[IngestionRun] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [ctl].[IngestionRun] already exists. No action taken.';
END;
GO

/*
===============================================================================
4. ctl.IngestionRunStep
===============================================================================
*/

IF OBJECT_ID(N'ctl.IngestionRunStep', N'U') IS NULL
BEGIN
    PRINT 'Creating table [ctl].[IngestionRunStep]...';

    CREATE TABLE ctl.IngestionRunStep
    (
        RunStepId       bigint IDENTITY(1,1) NOT NULL,
        RunId           uniqueidentifier NOT NULL,
        StepName        varchar(100) NOT NULL,
        ActivityName    varchar(150) NULL,
        Status          varchar(30) NOT NULL,
        RowsRead        bigint NULL,
        RowsCopied      bigint NULL,
        StartedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_IngestionRunStep_StartedAt DEFAULT (SYSUTCDATETIME()),
        EndedAt         datetime2(3) NULL,
        DurationSeconds int NULL,
        ErrorMessage    nvarchar(max) NULL,
        CreatedAt       datetime2(3) NOT NULL
            CONSTRAINT DF_IngestionRunStep_CreatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_IngestionRunStep
            PRIMARY KEY CLUSTERED (RunStepId),

        CONSTRAINT FK_IngestionRunStep_IngestionRun
            FOREIGN KEY (RunId)
            REFERENCES ctl.IngestionRun (RunId),

        CONSTRAINT CK_IngestionRunStep_Status
            CHECK (Status IN ('Started', 'Succeeded', 'Failed', 'Skipped')),

        CONSTRAINT CK_IngestionRunStep_RowsRead
            CHECK (RowsRead IS NULL OR RowsRead >= 0),

        CONSTRAINT CK_IngestionRunStep_RowsCopied
            CHECK (RowsCopied IS NULL OR RowsCopied >= 0),

        CONSTRAINT CK_IngestionRunStep_DurationSeconds
            CHECK (DurationSeconds IS NULL OR DurationSeconds >= 0)
    );

    PRINT 'Table [ctl].[IngestionRunStep] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [ctl].[IngestionRunStep] already exists. No action taken.';
END;
GO

/*
===============================================================================
5. ctl.WatermarkHistory
===============================================================================
*/

IF OBJECT_ID(N'ctl.WatermarkHistory', N'U') IS NULL
BEGIN
    PRINT 'Creating table [ctl].[WatermarkHistory]...';

    CREATE TABLE ctl.WatermarkHistory
    (
        WatermarkHistoryId     bigint IDENTITY(1,1) NOT NULL,
        SourceObjectId         int NOT NULL,
        RunId                  uniqueidentifier NOT NULL,
        PreviousWatermarkValue datetime2(3) NOT NULL,
        NewWatermarkValue      datetime2(3) NOT NULL,
        AppliedAt              datetime2(3) NOT NULL
            CONSTRAINT DF_WatermarkHistory_AppliedAt DEFAULT (SYSUTCDATETIME()),
        AppliedByPipelineRunId varchar(100) NULL,
        CreatedAt              datetime2(3) NOT NULL
            CONSTRAINT DF_WatermarkHistory_CreatedAt DEFAULT (SYSUTCDATETIME()),

        CONSTRAINT PK_WatermarkHistory
            PRIMARY KEY CLUSTERED (WatermarkHistoryId),

        CONSTRAINT FK_WatermarkHistory_SourceObject
            FOREIGN KEY (SourceObjectId)
            REFERENCES ctl.SourceObject (SourceObjectId),

        CONSTRAINT FK_WatermarkHistory_IngestionRun
            FOREIGN KEY (RunId)
            REFERENCES ctl.IngestionRun (RunId),

        CONSTRAINT CK_WatermarkHistory_WatermarkOrder
            CHECK (NewWatermarkValue >= PreviousWatermarkValue)
    );

    PRINT 'Table [ctl].[WatermarkHistory] created successfully.';
END
ELSE
BEGIN
    PRINT 'Table [ctl].[WatermarkHistory] already exists. No action taken.';
END;
GO

/*
===============================================================================
6. Validation — Control tables
===============================================================================
*/

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    t.create_date,
    t.modify_date
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
WHERE s.name = N'ctl'
  AND t.name IN
  (
      N'SourceObject',
      N'FileSourceConfig',
      N'IngestionRun',
      N'IngestionRunStep',
      N'WatermarkHistory'
  )
ORDER BY
    t.name;
GO

/*
===============================================================================
7. Validation — Control table foreign keys
===============================================================================
*/

SELECT
    fk.name AS ForeignKeyName,
    OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ChildSchema,
    OBJECT_NAME(fk.parent_object_id) AS ChildTable,
    OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ParentSchema,
    OBJECT_NAME(fk.referenced_object_id) AS ParentTable
FROM sys.foreign_keys AS fk
WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = N'ctl'
ORDER BY
    ChildTable,
    ForeignKeyName;
GO

/*
===============================================================================
8. Validation — Control table columns
===============================================================================
*/

SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    c.column_id,
    c.name AS ColumnName,
    ty.name AS DataType,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON t.schema_id = s.schema_id
INNER JOIN sys.columns AS c
    ON t.object_id = c.object_id
INNER JOIN sys.types AS ty
    ON c.user_type_id = ty.user_type_id
WHERE s.name = N'ctl'
  AND t.name IN
  (
      N'SourceObject',
      N'FileSourceConfig',
      N'IngestionRun',
      N'IngestionRunStep',
      N'WatermarkHistory'
  )
ORDER BY
    t.name,
    c.column_id;
GO