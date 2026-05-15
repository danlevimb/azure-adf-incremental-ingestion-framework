/*
===============================================================================
Project:        azure-adf-incremental-ingestion-framework
Script:         06_create_adf_sql_login.sql
Purpose:        Create a dedicated SQL Server login and database user for Azure
                Data Factory connectivity through Self-hosted Integration Runtime.

Database:       ADF_Ingestion_Source
Phase:          Phase 5 — ADF Connectivity Setup

Security notes:
- Do not commit real passwords to GitHub.
- Replace the placeholder password before executing.
- This login is intended for MVP development only.
===============================================================================
*/

USE master;
GO

/*
===============================================================================
1. Create SQL Server login
IMPORTANT:
- Replace the password before running.
- Do not commit the real password.
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.sql_logins
    WHERE name = N'adf_ingestion_user'
)
BEGIN
    CREATE LOGIN [adf_ingestion_user]
    WITH PASSWORD = 'REPLACE_WITH_STRONG_LOCAL_PASSWORD',
         CHECK_POLICY = ON,
         CHECK_EXPIRATION = OFF;

    PRINT 'Login [adf_ingestion_user] created successfully.';
END
ELSE
BEGIN
    PRINT 'Login [adf_ingestion_user] already exists. No action taken.';
END;
GO

USE [ADF_Ingestion_Source];
GO

/*
===============================================================================
2. Create database user
===============================================================================
*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'adf_ingestion_user'
)
BEGIN
    CREATE USER [adf_ingestion_user]
    FOR LOGIN [adf_ingestion_user];

    PRINT 'User [adf_ingestion_user] created successfully.';
END
ELSE
BEGIN
    PRINT 'User [adf_ingestion_user] already exists. No action taken.';
END;
GO

/*
===============================================================================
3. Grant permissions for source reads
===============================================================================
*/

GRANT SELECT ON SCHEMA::dbo TO [adf_ingestion_user];
GO

/*
===============================================================================
4. Grant permissions for control metadata operations
===============================================================================
*/

GRANT SELECT, INSERT, UPDATE ON SCHEMA::ctl TO [adf_ingestion_user];
GO

/*
===============================================================================
5. Grant execute permissions for control stored procedures
===============================================================================
*/

GRANT EXECUTE ON SCHEMA::ctl TO [adf_ingestion_user];
GO

/*
===============================================================================
6. Validation
===============================================================================
*/

SELECT
    dp.name AS DatabaseUserName,
    dp.type_desc,
    dp.authentication_type_desc,
    dp.create_date
FROM sys.database_principals AS dp
WHERE dp.name = N'adf_ingestion_user';
GO

SELECT
    pr.state_desc,
    pr.permission_name,
    SCHEMA_NAME(major_id) AS SchemaName,
    USER_NAME(grantee_principal_id) AS GranteeName
FROM sys.database_permissions AS pr
WHERE USER_NAME(pr.grantee_principal_id) = N'adf_ingestion_user'
ORDER BY
    SchemaName,
    permission_name;
GO