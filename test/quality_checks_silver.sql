/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;










---Step 1: Quality check
---Check for null or duplicate in Primary key

SELECT cust_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL


SELECT *
FROM bronze.crm_cust_info
WHERE cust_id = 29483


SELECT *
FROM
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cust_id IS NOT NULL
)t
WHERE flag_last = 1

SELECT *
FROM
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info
)t
WHERE flag_last != 1


---For string: check unwanted spaces---for all string data types in table 

SELECT * FROM bronze.crm_cust_info
SELECT
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT
	cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)  ---No Space


SELECT
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)  ---No Space

SELECT
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key) ---No Space

--Cleaned firstname and lastname
SELECT 
	cust_id,
	cst_key,
	TRIM(cst_firstname) AS firstname,
	TRIM(cst_lastname) AS lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cust_id IS NOT NULL
)t
WHERE flag_last = 1


--Data Standardization and Consistency 1:01:35:19

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info



SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;


INSERT INTO silver.crm_cust_info (
		cust_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
)

SELECT 
	cust_id,
	cst_key,
	TRIM(cst_firstname) AS firstname,
	TRIM(cst_lastname) AS lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cst_create_date DESC ) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cust_id IS NOT NULL
)t
WHERE flag_last = 1;

SELECT * FROM silver.crm_cust_info

SELECT 
	cust_id,
	COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) >1 OR cust_id IS NULL



SELECT
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT
	cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)  ---No Space


SELECT
	cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)  ---No Space

--Data Standardization and Consistency 1:01:35:19

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info



SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT *
FROM silver.crm_cust_info;


-------------------------crm_prod_info ----Quality checks

SELECT * FROM bronze.crm_prd_info

SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL ----No Duplicate


SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) ---No space


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') NOT IN (SELECT id FROM bronze.erp_px_cat_g1v2) ---TO VALIDATE 

SELECT id FROM bronze.erp_px_cat_g1v2 ----Id has '_' and bronze.crm_prd_info cat_id has '-'



SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7 , LENGTH(prd_key)) NOT IN 
	(SELECT sls_prd_key FROM bronze.crm_sales_details
	WHERE sls_prd_key LIKE 'FK%')


	

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7 , LENGTH(prd_key)) IN 
	(SELECT sls_prd_key FROM bronze.crm_sales_details)


---For ord_cost we can check -ve number or NULL
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info

---prd_line
SELECT DISTINCT prd_line FROM  bronze.crm_prd_info


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


---Other way of case

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))----Effiecient way only for mapping values not for complex
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info


----Check for invvalid Order date
SELECT  * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt ---Many incorrect dates --Brain storm and ask the expert (Use next start date as end date of prevoius product)

--Check and test logic for 2 product key 
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))----Effiecient way only for mapping values not for complex
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509' )



SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))----Effiecient way only for mapping values not for complex
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt
FROM bronze.crm_prd_info


---Remove time from date column


SELECT
	prd_id,
	prd_key,
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))----Effiecient way only for mapping values not for complex
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	CAST (prd_start_dt AS DATE),
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE)AS prd_end_dt
FROM bronze.crm_prd_info

--Since we changed prd_key into cat_key and prd_key , also date dataype is change to date instead of datetime
--Need to change DDL database structure
--bEFORE

CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost FLOAT,
	prd_line VARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATE,
	dwh_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

--After
DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info
(
	prd_id INT,
	prd_key VARCHAR(50),
	cat_id VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost FLOAT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

SELECT * FROM silver.crm_prd_info


INSERT INTO silver.crm_prd_info
(
    prd_id,
	prd_key,
	cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	SUBSTRING(prd_key, 7 , LENGTH(prd_key)) AS prd_key,
	REPLACE(SUBSTRING (prd_key,1,5), '-' , '_') AS cat_id,---Need to split and create cat_id to map with erp_px_cat table
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))----Effiecient way only for mapping values not for complex
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'other Sales'
		 WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END prd_line,
	CAST (prd_start_dt AS DATE),
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE)AS prd_end_dt
FROM bronze.crm_prd_info


SELECT * FROM silver.crm_prd_info

---Check again the Quality

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) ---No space


SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info

------crm_sales_details
SELECT * FROM bronze.crm_sales_details




SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM (sls_ord_num);


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cust_id FROM silver.crm_cust_info)

SELECT * FROM silver.crm_cust_info


---Check for valid date

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <0

SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

--Replacing 0 with NULL
SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 OR sls_order_dt > 20500101

--Checking the range of date 
SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101

SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR 
LENGTH(CAST(sls_order_dt AS TEXT)) != 8 OR 
sls_order_dt > 20500101
OR sls_order_dt < 19000101



SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;


SELECT 
NULLIF (sls_ship_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR 
LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 OR 
sls_ship_dt > 20500101
OR sls_ship_dt < 19000101


SELECT 
NULLIF (sls_due_dt ,0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR 
LENGTH(CAST(sls_due_dt AS TEXT)) != 8 OR 
sls_due_dt > 20500101
OR sls_due_dt < 19000101

---Orderdate should be smaller than shiping and due date
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	
    CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	
    CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;


SELECT sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0  OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price

---Checking Sales = quantity * price. Note there should be no null, negative value or 0
SELECT DISTINCT sls_sales AS old_sales,
    sls_quantity,
    sls_price AS old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales / NULLIF(sls_quantity , 0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details



SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
	CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	
    CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	
    CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales / NULLIF(sls_quantity , 0)
	 ELSE sls_price
END AS sls_price,
    sls_quantity
FROM bronze.crm_sales_details;

DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

INSERT INTO silver.crm_sales_details
(
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
	CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	
    CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	
    CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0
	 THEN sls_sales / NULLIF(sls_quantity , 0)
	 ELSE sls_price
END AS sls_price,
    sls_quantity
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details
--Quality check
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)




SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM (sls_ord_num);


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)


SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cust_id FROM silver.crm_cust_info)

SELECT * FROM silver.crm_cust_info


---Check for valid date

SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt < 0

SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0

--Replacing 0 with NULL
SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0

SELECT *
--NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 OR sls_order_dt > 20500101

--Checking the range of date 
SELECT 
--NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt < 19000101

SELECT 
NULLIF (sls_order_dt ,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR 
LENGTH(CAST(sls_order_dt AS TEXT)) != 8 OR 
sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0  OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity,sls_price

--------------------------------------------
SELECT * FROM bronze.erp_cust_az12
---------------------------
---CID WILL JOIN CRM_CUST_INFO , CHECK FOR ID AND THEN ALTER

SELECT cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00%'

SELECT cust_key
FROM silver.crm_cust_info

SELECT cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE cid LIKE 'AW%'

SELECT cid AS old_cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
	 END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
	 END AS cid
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
	 END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)



SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 	 ELSE cid
	 END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12


SELECT cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid)

SELECT cid,
	COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL

SELECT 
	DISTINCT gen
FROM bronze.erp_cust_az12


--Identify out of range date

SELECT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE  --cust older than 100yer


SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 	 ELSE cid
	 END AS cid,
	CASE WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
	END AS bdate,
	gen
FROM bronze.erp_cust_az12



SELECT 
	DISTINCT gen
FROM bronze.erp_cust_az12

SELECT 
	DISTINCT gen AS old_gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'FEMALE'
		WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'MALE'
		ELSE 'n/a'
		END gen	
FROM bronze.erp_cust_az12

SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 	 ELSE cid
	END AS cid,
	
	CASE WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
	END AS bdate,
	
	CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'FEMALE'
		WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'MALE'
		ELSE 'n/a'
		END gen	
FROM bronze.erp_cust_az12

---DDL and iNsertion since Dataype are correct no need to change database 

INSERT INTO silver.erp_cust_az12 (
 cid,
 bdate,
 gen
)
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 	 ELSE cid
	END AS cid,
	
	CASE WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
	END AS bdate,
	
	CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'FEMALE'
		WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'MALE'
		ELSE 'n/a'
		END gen	
FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12



-----------Quality Check

SELECT cid,
	bdate,
	gen
FROM silver.erp_cust_az12
WHERE cid LIKE 'NA%'



SELECT
cid
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)




SELECT cid,
	bdate,
	gen
FROM silver.erp_cust_az12
WHERE cid != TRIM(cid)

SELECT cid,
	COUNT(*)
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL

SELECT 
	DISTINCT gen
FROM silver.erp_cust_az12


--Identify out of range date

SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE  --cust older than 100yer

SELECT * FROM silver.erp_cust_az12




----------------------------------erp_loc_a101
--Comparing cid and cst_key wot join the table , cid has '-'
SELECT cid,cntry FROM bronze.erp_loc_a101

SELECT cst_key FROM silver.crm_cust_info


SELECT cid AS old_cid,
REPLACE(cid ,'-' ,'') AS cid,
cntry 
FROM bronze.erp_loc_a101


SELECT cid AS old_cid,
REPLACE(cid ,'-' ,'') AS cid,
cntry 
FROM bronze.erp_loc_a101 
WHERE REPLACE(cid ,'-' ,'') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT cid, COUNT(*)
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL




SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101


SELECT
DISTINCT cntry AS old_cnt,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('USA' , 'US') THEN 'United States'
	WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101




SELECT
	REPLACE(cid ,'-' ,'') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('USA' , 'US') THEN 'United States'
		 WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101


INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
	REPLACE(cid ,'-' ,'') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('USA' , 'US') THEN 'United States'
		 WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101

SELECT * FROM silver.erp_loc_a101

---qUALITY CHECKS
SELECT cid
cntry 
FROM silver.erp_loc_a101 
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

SELECT DISTINCT cntry 
FROM silver.erp_loc_a101


------------exp_px_cat_g1v2
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT id,
	cat,
	subcat,
	maintenance 
FROM bronze.erp_px_cat_g1v2

SELECT id,COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 AND id IS NULL


SELECT * FROM silver.crm_prd_info  ---cat id is matching with id

SELECT cat,subcat,maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2
GROUP BY cat

SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2
GROUP BY maintenance

TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

SELECT id,
	cat,
	subcat,
	maintenance 
FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2

SELECT cat,subcat,maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2
GROUP BY cat

SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2
GROUP BY maintenance

