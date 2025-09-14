--------------------------------------------------------------
-- File: qualitycheck.sql
-- Purpose: Bronze -> Silver ETL + Quality Checks (standardized)
-- Author: Shashi Bhushan
-- Last Modified: 14-Sep-2025
-- Notes: Combined pipelines for all tables you shared. Logic preserved.
--------------------------------------------------------------

USE DataWarehouse;
GO

/* ===================================================================
   TABLE: crm_sales_details  (Bronze → Silver)
   - Convert integer dates (YYYYMMDD) → DATE (invalid → NULL)
   - Fix sales/price logic:
       * If sales NULL/<=0 or mismatched → recalc as quantity * ABS(price)
       * If price NULL/<=0 → derive as sales/quantity
       * Ensure price is positive
   - Pre-load & post-load quality checks
   =================================================================== */
--------------------------------------------------------------
-- Step 1: Explore Source (Bronze Layer)
--------------------------------------------------------------
SELECT TOP 10 * 
FROM bronze.crm_sales_details;
-- Check sample records in Bronze layer.

--------------------------------------------------------------
-- Step 2: Data Cleaning & Transformation Logic (Preview)
--------------------------------------------------------------
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Order Date
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
             OR sls_order_dt >= 20500101 OR sls_order_dt <= 19000101
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE)
    END AS sls_order_dt,

    -- Ship Date
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE)
    END AS sls_ship_dt,

    -- Due Date
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE)
    END AS sls_due_dt,

    -- Fix Sales
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    -- Quantity
    sls_quantity,

    -- Fix Price
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE ABS(sls_price)
    END AS sls_price

FROM bronze.crm_sales_details;


--------------------------------------------------------------
-- Step 3: Data Quality Validations (Before Load)
--------------------------------------------------------------

-- 3.1 Row Count
SELECT COUNT(*) AS bronze_rowcount 
FROM bronze.crm_sales_details;

-- 3.2 Invalid Product Keys
SELECT COUNT(*) AS invalid_product_keys
FROM bronze.crm_sales_details s
WHERE s.sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- 3.3 Invalid Customer IDs
SELECT COUNT(*) AS invalid_customer_ids
FROM bronze.crm_sales_details s
WHERE s.sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- 3.4 Invalid/Malformed Dates
SELECT COUNT(*) AS invalid_dates
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 
   OR sls_order_dt >= 20500101 OR sls_order_dt <= 19000101;

-- 3.5 Logical Date Errors: Order after Ship/Due
SELECT COUNT(*) AS date_order_errors
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- 3.6 Negative/NULL/Zero Sales or Price
SELECT COUNT(*) AS invalid_sales_or_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
   OR sls_sales != sls_quantity * sls_price;


--------------------------------------------------------------
-- Step 4: Load into Silver Layer (full refresh)
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
             OR sls_order_dt >= 20500101 OR sls_order_dt <= 19000101
        THEN NULL
        ELSE CAST(CAST(sls_order_dt AS CHAR(8)) AS DATE)
    END AS sls_order_dt,

    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS CHAR(8)) AS DATE)
    END AS sls_ship_dt,

    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
        THEN NULL
        ELSE CAST(CAST(sls_due_dt AS CHAR(8)) AS DATE)
    END AS sls_due_dt,

    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE ABS(sls_price)
    END AS sls_price
FROM bronze.crm_sales_details;


--------------------------------------------------------------
-- Step 5: Post-Load Validation (Silver Layer)
--------------------------------------------------------------

-- 5.1 Row Count Comparison (Bronze vs Silver)
SELECT 
    (SELECT COUNT(*) FROM bronze.crm_sales_details) AS bronze_count,
    (SELECT COUNT(*) FROM silver.crm_sales_details) AS silver_count;

-- 5.2 Null Checks on PK Columns
SELECT COUNT(*) AS null_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;

SELECT COUNT(*) AS null_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key IS NULL;

SELECT COUNT(*) AS null_cust_id
FROM silver.crm_sales_details
WHERE sls_cust_id IS NULL;

-- 5.3 Duplicate Check (Order + Product + Customer should be unique)
SELECT sls_ord_num, sls_prd_key, sls_cust_id, COUNT(*) AS dup_count
FROM silver.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key, sls_cust_id
HAVING COUNT(*) > 1;

-- 5.4 Logical Date Validation
SELECT COUNT(*) AS invalid_date_logic
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- 5.5 Final Silver Preview
SELECT TOP 20 * 
FROM silver.crm_sales_details;


--------------------------------------------------------------------------------
/* ===================================================================
   TABLE: crm_prd_info  (Bronze → Silver)
   - Clean & standardize product keys & names
   - Replace NULL prd_cost → 0
   - Map prd_line codes to descriptive values
   - Calculate prd_end_dt as LEAD(prd_start_dt) - 1
   =================================================================== */
--------------------------------------------------------------
-- 1. Bronze Exploration
--------------------------------------------------------------
SELECT TOP 1000 * FROM bronze.crm_prd_info;
SELECT TOP 1000 * FROM bronze.crm_sales_details;
SELECT TOP 1000 * FROM bronze.erp_cust_az12;
SELECT TOP 1000 * FROM bronze.erp_loc_a101;
SELECT TOP 1000 * FROM bronze.erp_px_cat_g1v2;

--------------------------------------------------------------
-- 2. Data Quality Checks in Bronze
--------------------------------------------------------------
-- 2.1 Check for unwanted spaces in product name
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 2.2 Check for invalid/NULL product cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 2.3 Check distinct product line values for standardization
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- 2.4 Check invalid date ordering
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- 2.5 Validate lifecycle (prd_end_dt vs next prd_start_dt)
SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


--------------------------------------------------------------
-- 3. Data Transformation / Cleaning (Preview)
--------------------------------------------------------------
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  --Extract category ID
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,         --Extract product key
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost, --Null values replaced with 0.
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line, --Map product line codes to descriptive values
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
    AS DATE) AS prd_end_dt  --Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;


--------------------------------------------------------------
-- 4. Load into Silver Layer
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting Data into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'Other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


--------------------------------------------------------------
-- 5. Re-Validation in Silver (Test Cases)
--------------------------------------------------------------
-- 5.1 Check for duplicates in primary key (prd_id)
SELECT 
    prd_id,
    COUNT(*) AS count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- 5.2 Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- 5.3 Validate product cost
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- 5.4 Validate standardized product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- 5.5 Check date consistency
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- 5.6 Final view of cleaned data
SELECT TOP 1000 * FROM silver.crm_prd_info;


--------------------------------------------------------------------------------
/* ===================================================================
   TABLE: erp_px_cat_g1v2  (Bronze → Silver)
   - Clean maintenance field spaces and insert to silver.erp_px_cat_g1v2
   =================================================================== */
--------------------------------------------------------------
-- Step 1: Validate Bronze Data Before Insert
--------------------------------------------------------------
-- Check for unwanted spaces in 'maintenance'
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE maintenance != LTRIM(RTRIM(maintenance));

-- Check distinct values of 'maintenance' for standardization
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

-- Check for IDs already existing in Silver
SELECT b.id
FROM bronze.erp_px_cat_g1v2 b
WHERE b.id IN (
    SELECT DISTINCT cat_id
    FROM silver.crm_prd_info
);


--------------------------------------------------------------
-- Step 2: Insert New Records into Silver
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT
    b.id,
    b.cat,
    b.subcat,
    LTRIM(RTRIM(b.maintenance)) AS maintenance -- clean spaces during insert
FROM bronze.erp_px_cat_g1v2 b;


--------------------------------------------------------------
-- Step 3: Re-Validate Silver After Insert
--------------------------------------------------------------
-- Verify inserted records
SELECT *
FROM silver.erp_px_cat_g1v2
ORDER BY id;

-- Check for any rows with unwanted spaces (should be none after insert)
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE maintenance != LTRIM(RTRIM(maintenance));

-- Check distinct values of 'maintenance' in Silver
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;


-- Find bronze rows whose id is not present in silver.crm_prd_info.cat_id
SELECT
    b.id,
    b.cat,
    b.subcat,
    LTRIM(RTRIM(b.maintenance)) AS maintenance
FROM bronze.erp_px_cat_g1v2 b
WHERE b.id NOT IN (
    SELECT DISTINCT cat_id
    FROM silver.crm_prd_info
);


--------------------------------------------------------------------------------
/* ===================================================================
   TABLE: erp_loc_a101  (Bronze → Silver)
   - Standardize Customer ID (remove dashes) and normalize country values
   - Country mapping: DE -> Germany, US/USA -> United States, NULL/empty -> 'n/a'
   =================================================================== */
--------------------------------------------------------------
-- Step 1: Preview Transformed Data (Before Insert)
--------------------------------------------------------------
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101 b
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cid 
    FROM silver.erp_loc_a101
);

--------------------------------------------------------------
-- Step 2: Data Standardization Validation
--------------------------------------------------------------
SELECT DISTINCT 
    cntry AS old_cntry,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS new_cntry
FROM bronze.erp_loc_a101
ORDER BY new_cntry;

--------------------------------------------------------------
-- Step 3: Row Count Validation Before Insert
--------------------------------------------------------------
SELECT COUNT(*) AS silver_count_before
FROM silver.erp_loc_a101;

--------------------------------------------------------------
-- Step 4: Load Clean Data into Silver
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;

PRINT '>> Inserting Clean Data into: silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

--------------------------------------------------------------
-- Step 5: Row Count Validation After Insert
--------------------------------------------------------------
SELECT COUNT(*) AS silver_count_after
FROM silver.erp_loc_a101;

--------------------------------------------------------------
-- Step 6: Duplicate & Data Quality Validation
--------------------------------------------------------------
-- 6.1 Check Duplicate CIDs
SELECT 
    cid, COUNT(*) AS duplicate_count
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1;

-- 6.2 Check for Invalid Countries
SELECT *
FROM silver.erp_loc_a101
WHERE cntry IS NULL OR cntry = '';

--------------------------------------------------------------
-- Step 7: Preview Final Silver Data
--------------------------------------------------------------
SELECT TOP 50 * 
FROM silver.erp_loc_a101;


--------------------------------------------------------------------------------
/* ===================================================================
   TABLE: erp_cust_az12  (Bronze → Silver)
   - Transformations:
       * Remove 'NAS' prefix from cid
       * bdate -> NULL if > GETDATE()
       * Normalize gen -> 'Female'/'Male'/'n/a'
   - Validations and load
   =================================================================== */
--------------------------------------------------------------
-- Step 1: Explore Source (Bronze Layer)
--------------------------------------------------------------
SELECT TOP 10 * 
FROM bronze.erp_cust_az12;
-- Sample data check

--------------------------------------------------------------
-- Step 2: Transformation Rules (Preview)
--------------------------------------------------------------
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
        ELSE cid 
    END AS cid,

    CASE 
        WHEN bdate > GETDATE() THEN NULL 
        ELSE bdate 
    END AS bdate,

    CASE 
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;


--------------------------------------------------------------
-- Step 3: Data Quality Validations (Before Load)
--------------------------------------------------------------

-- 3.1 Row Count
SELECT COUNT(*) AS bronze_rowcount 
FROM bronze.erp_cust_az12;

-- 3.2 Invalid Birthdates (before 1924 or in future)
SELECT COUNT(*) AS invalid_birthdates
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- 3.3 Distinct Gender Values & Mapped Values
SELECT DISTINCT 
    gen AS raw_gen,
    CASE 
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS mapped_gen
FROM bronze.erp_cust_az12;

-- 3.4 Customers Not in CRM Customer Master
SELECT COUNT(*) AS unmapped_customers
FROM bronze.erp_cust_az12 b
WHERE (CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END)
      NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);


--------------------------------------------------------------
-- Step 4: Load into Silver Layer
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
        ELSE cid 
    END AS cid,

    CASE 
        WHEN bdate > GETDATE() THEN NULL 
        ELSE bdate 
    END AS bdate,

    CASE 
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;


--------------------------------------------------------------
-- Step 5: Post-Load Validation (Silver Layer)
--------------------------------------------------------------

-- 5.1 Row Count Comparison
SELECT 
    (SELECT COUNT(*) FROM bronze.erp_cust_az12) AS bronze_count,
    (SELECT COUNT(*) FROM silver.erp_cust_az12) AS silver_count;

-- 5.2 Null Checks (CID should not be NULL)
SELECT COUNT(*) AS null_cid_count
FROM silver.erp_cust_az12
WHERE cid IS NULL;

-- 5.3 Duplicate Check (CID should be unique)
SELECT cid, COUNT(*) AS dup_count
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- 5.4 Invalid Birthdate Check
SELECT COUNT(*) AS invalid_birthdates_after_load
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- 5.5 Gender Distribution
SELECT gen, COUNT(*) AS count_by_gender
FROM silver.erp_cust_az12
GROUP BY gen;

-- 5.6 Preview
SELECT TOP 20 * 
FROM silver.erp_cust_az12;


--------------------------------------------------------------------------------
/* ===================================================================
   TABLE: crm_cust_info  (Bronze → Silver)
   - Deduplicate bronze using ROW_NUMBER() keeping latest record by create date
   - Trim name fields, normalize gender & marital status codes
   - Validate and insert into silver.crm_cust_info
   =================================================================== */
--------------------------------------------------------------
-- Step 1: Quick Sampling (Top 1000 records from Bronze Layer)
--------------------------------------------------------------
SELECT TOP 1000 * FROM bronze.crm_cust_info;
SELECT TOP 1000 * FROM bronze.crm_prd_info;
SELECT TOP 1000 * FROM bronze.crm_sales_details;

--------------------------------------------------------------
-- Step 2: Check for Nulls or Duplicates in Bronze Layer PK
--------------------------------------------------------------
-- (A) Duplicate check (does NOT detect NULLs)
SELECT  
    cst_id,
    COUNT(cst_id) AS count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1;

-- (B) Duplicate + NULL check
SELECT  
    cst_id,
    COUNT(*) AS count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


--------------------------------------------------------------
-- Step 3: Deduplicate using ROW_NUMBER()
-- Logic: Keep the latest record (by create date) per customer
--------------------------------------------------------------
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1
  AND cst_id IS NOT NULL;


--------------------------------------------------------------
-- Step 4: Data Quality Checks - Unwanted Spaces (Bronze Layer)
--------------------------------------------------------------
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr <> TRIM(cst_gndr);


--------------------------------------------------------------
-- Step 5: Profiling - Distinct Values (Bronze Layer)
--------------------------------------------------------------
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

SELECT MIN(cst_create_date) AS min_date,
       MAX(cst_create_date) AS max_date
FROM bronze.crm_cust_info;


--------------------------------------------------------------
-- Step 6: Insert Clean Data into Silver Layer
--------------------------------------------------------------
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data into: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1
  AND cst_id IS NOT NULL;


--------------------------------------------------------------
-- Step 7: Re-check Data Quality in Silver Layer
--------------------------------------------------------------
-- (A) Duplicate check (without null check)
SELECT  
    cst_id,
    COUNT(cst_id) AS count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1;

-- (B) Duplicate + NULL check
SELECT  
    cst_id,
    COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Deduplication verification (select latest per cst_id)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM silver.crm_cust_info
) t
WHERE flag_last = 1
  AND cst_id IS NOT NULL;

-- Space checks
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname <> TRIM(cst_firstname);
SELECT cst_lastname  FROM silver.crm_cust_info WHERE cst_lastname  <> TRIM(cst_lastname);
SELECT cst_gndr      FROM silver.crm_cust_info WHERE cst_gndr      <> TRIM(cst_gndr);

-- Profiling distinct values
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

-- Final Silver Layer records
SELECT * FROM silver.crm_cust_info;

--------------------------------------------------------------
-- End of qualitycheck.sql
--------------------------------------------------------------
