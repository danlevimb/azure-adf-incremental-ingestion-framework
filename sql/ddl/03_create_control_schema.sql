/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         03_create_control_schema.sql
Purpose:        Create the control metadata schema used by the ADF incremental
                ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Schema:
- ctl

Notes:
- This script is intentionally non-destructive.
- It creates the schema only if it does not already exist.
- The ctl schema will store framework metadata tables and stored procedures.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
1. Create control schema
===============================================================================
*/

IF SCHEMA_ID(N'ctl') IS NULL
BEGIN
    PRINT 'Creating schema [ctl]...';

    EXEC(N'CREATE SCHEMA ctl AUTHORIZATION dbo;');

    PRINT 'Schema [ctl] created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema [ctl] already exists. No action taken.';
END;
GO

/*
===============================================================================
2. Validation — Control schema
===============================================================================
*/

SELECT
    s.name AS SchemaName,
    USER_NAME(s.principal_id) AS SchemaOwner
FROM sys.schemas AS s
WHERE s.name = N'ctl';
GO