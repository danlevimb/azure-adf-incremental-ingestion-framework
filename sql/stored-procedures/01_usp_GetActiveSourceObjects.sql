/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         01_usp_GetActiveSourceObjects.sql
Purpose:        Return active source objects that should be processed by the
                master ADF ingestion orchestrator.

Database:       ADF_Ingestion_Source
Phase:          Phase 3 — Control Metadata Layer

Procedure:
- ctl.usp_GetActiveSourceObjects

Used by:
- PL_00_Master_Ingestion_Orchestrator

Notes:
- This procedure is read-only.
- It supports optional filtering by SourceSystemName.
- It supports RunMode filtering for ALL, SQL_ONLY, and FILES_ONLY.
===============================================================================
*/

USE [ADF_Ingestion_Source];
GO

CREATE OR ALTER PROCEDURE ctl.usp_GetActiveSourceObjects
(
    @SourceSystemName varchar(100) = NULL,
    @RunMode          varchar(30)  = 'ALL'
)
AS
BEGIN
    SET NOCOUNT ON;

    /*
    ===========================================================================
    Validate inputs
    ===========================================================================
    */

    IF @RunMode NOT IN ('ALL', 'SQL_ONLY', 'FILES_ONLY')
    BEGIN
        THROW 50001, 'Invalid @RunMode. Expected values: ALL, SQL_ONLY, FILES_ONLY.', 1;
    END;

    /*
    ===========================================================================
    Return active source objects
    ===========================================================================
    */

    SELECT
        SourceObjectId,
        SourceSystemName,
        SourceType,
        SourceSchema,
        SourceObjectName,
        LoadType,
        DestinationContainer,
        DestinationFolder,
        DestinationFormat
    FROM ctl.SourceObject
    WHERE IsActive = 1
      AND
      (
          @SourceSystemName IS NULL
          OR SourceSystemName = @SourceSystemName
      )
      AND
      (
          @RunMode = 'ALL'
          OR (@RunMode = 'SQL_ONLY' AND SourceType = 'SQL_TABLE')
          OR (@RunMode = 'FILES_ONLY' AND SourceType IN ('CSV_FILE', 'JSON_FILE'))
      )
    ORDER BY
        CASE
            WHEN SourceType = 'SQL_TABLE' THEN 1
            WHEN SourceType = 'CSV_FILE' THEN 2
            WHEN SourceType = 'JSON_FILE' THEN 3
            ELSE 4
        END,
        SourceSystemName,
        SourceSchema,
        SourceObjectName;
END;
GO

/*
===============================================================================
Validation — Active source objects
Compact output for evidence
===============================================================================
*/

EXEC ctl.usp_GetActiveSourceObjects
    @SourceSystemName = NULL,
    @RunMode = 'ALL';
GO

/*
===============================================================================
Validation — SQL only
===============================================================================
*/

EXEC ctl.usp_GetActiveSourceObjects
    @SourceSystemName = 'sales_local',
    @RunMode = 'SQL_ONLY';
GO

/*
===============================================================================
Validation — Files only
===============================================================================
*/

EXEC ctl.usp_GetActiveSourceObjects
    @SourceSystemName = 'reference_files',
    @RunMode = 'FILES_ONLY';
GO