/*
============================================================
Quality Checks
============================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
        - Null or duplicate primary keys.
        - Unwanted spaces in string fields.
        - Data standardization and consistency.
        - Invalid date ranges and orders.
        - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
============================================================
*/

-- ===========================================
-- Checking 'silver.crm_cust_info' 
-- ===========================================
-- Check for NULLS or duplicates in Primary Key
-- Expectation: No Result
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_lastname);

-- Data Standardization and consistency 
SELECT DISTINCT cst_material_status 
FROM silver.crm_cust_info;

-- ===========================================
-- Checking 'silver.crm_prd_info'
-- ===========================================
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT 
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization and consistency 
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info;

-- Check Invalid Date Orders 
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ===========================================
-- Checking 'silver.crm_sales_details' 
-- ===========================================
-- Check for Invalid Dates 
SELECT 
    NULLIF(sls_order_dt,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101;

SELECT 
    NULLIF(sls_ship_dt,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101;

SELECT 
    NULLIF(sls_due_dt,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT DISTINCT 
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    END AS sls_sales,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF (sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- ===========================================
-- Checking 'silver.erp_cust_az12' 
-- ===========================================
-- Identify Out-of-Range Dates
SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data Standardization and Consistency
SELECT DISTINCT 
    gen,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM silver.erp_cust_az12;

-- ===========================================
-- Checking 'silver.erp_loc_a101'
-- ===========================================
-- Data standardization and consistency
SELECT DISTINCT 
    cntry AS old_cntry,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ===========================================
-- Checking 'silver.erp_px_cat_g1v2' 
-- ===========================================
-- Check for unwanted spaces 
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
