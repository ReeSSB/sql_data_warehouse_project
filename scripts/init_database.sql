/* =====================================================================
   Script: create_database.sql
   Purpose: Initialize the Data Warehouse database and create schemas
   Author: [Your Name]
   =====================================================================

   ⚠️ WARNING:
   - This script will DROP the 'DataWarehouse' database if it already exists.
   - All existing data in 'DataWarehouse' will be permanently deleted.
   - Run this script carefully, especially in production environments.

   Usage:
   - Execute in SQL Server Management Studio (SSMS) or Azure Data Studio.
   - Run step by step if you want to avoid dropping existing databases.
   ===================================================================== */

-- Step 1: Switch to the master database
USE master;
GO

-- Step 2: Drop 'DataWarehouse' if it already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Set database to SINGLE_USER to ensure no active connections
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the database
    DROP DATABASE DataWarehouse;

    PRINT '⚠️ Existing DataWarehouse database dropped successfully.';
END;
GO

-- Step 3: Create a fresh 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

PRINT '✅ New DataWarehouse database created successfully.';

-- Step 4: Switch context to the newly created database
USE DataWarehouse;
GO

/* =====================================================================
   Create Schemas for Medallion Architecture
   Bronze  : Raw data (as ingested from sources like CSV/ERP/CRM).
   Silver  : Cleaned, standardized, and transformed data.
   Gold    : Business-ready data models (fact and dimension tables).
   ===================================================================== */

-- Create Bronze Schema
CREATE SCHEMA bronze;
GO
PRINT '✅ Bronze schema created.';

-- Create Silver Schema
CREATE SCHEMA silver;
GO
PRINT '✅ Silver schema created.';

-- Create Gold Schema
CREATE SCHEMA gold;
GO
PRINT '✅ Gold schema created.';

/* =====================================================================
   Script Completed
   Next Steps:
   - Load raw data into Bronze layer.
   - Apply transformations and move curated data to Silver layer.
   - Model data into star schema in Gold layer for reporting/analytics.
   ===================================================================== */
