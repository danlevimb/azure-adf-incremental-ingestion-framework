/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         01_create_database.sql
Purpose:        Create the local SQL Server source database for the ADF
                incremental ingestion framework.

Database:       ADF_Ingestion_Source
Phase:          Phase 2 — SQL Server Local Setup

Notes:
- This script is intentionally non-destructive.
- It creates the database only if it does not already exist.
- It does not drop or overwrite an existing database.
===============================================================================
*/

USE master;
GO

IF DB_ID(N'ADF_Ingestion_Source') IS NULL
BEGIN
    PRINT 'Creating database [ADF_Ingestion_Source]...';

    CREATE DATABASE [ADF_Ingestion_Source];

    PRINT 'Database [ADF_Ingestion_Source] created successfully.';
END
ELSE
BEGIN
    PRINT 'Database [ADF_Ingestion_Source] already exists. No action taken.';
END;
GO

/*
===============================================================================
Basic database configuration
===============================================================================
*/

ALTER DATABASE [ADF_Ingestion_Source]
SET RECOVERY SIMPLE;
GO

ALTER DATABASE [ADF_Ingestion_Source]
SET READ_COMMITTED_SNAPSHOT ON
WITH ROLLBACK IMMEDIATE;
GO

/*
===============================================================================
Validation
===============================================================================
*/

SELECT
    name AS DatabaseName,
    database_id,
    create_date,
    compatibility_level,
    recovery_model_desc,
    is_read_committed_snapshot_on
FROM sys.databases
WHERE name = N'ADF_Ingestion_Source';
GO