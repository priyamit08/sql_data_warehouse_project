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

-------------------------------------------------------------------------------------------
------------------Inserting Data in to Silver layer----------------------------------------

---------------------------------CRM--------------------------------------------------
EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Silver layer';
		PRINT '=========================================';
	
		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>Inserting Data Into:silver.crm_cust_info';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>Inserting Data Into:silver.crm_prd_info';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>Inserting Data Into:silver.crm_sales_details';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		
		-------------------ERP--------------------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>Inserting Data Into:silver.erp_cust_az12';
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
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		
		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Data Into:silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid ,'-' ,'') AS cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('USA' , 'US') THEN 'United States'
				 WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Data Into:silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		
		SELECT id,
			cat,
			subcat,
			maintenance 
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @batch_end_time = GETDATE();
		PRINT '==================================================';
		PRINT 'Loading Bronze layer is completed';
		PRINT '  - Total Load Duration' + CAST(DATEDIFF(second , @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds'
		PRINT '==================================================';
		END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '==========================================';
	END CATCH
END

-----------Postgesql------------
DO $$
BEGIN
    RAISE NOTICE '>> Truncating the Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance 
    FROM bronze.erp_px_cat_g1v2;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM silver.erp_px_cat_g1v2
