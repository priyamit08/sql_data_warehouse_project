/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading bronze layer';
		PRINT '=========================================';
	
		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
	
		PRINT '>> Inserting data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		
		PRINT '>> Inserting data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
	
		PRINT '>> Inserting data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '-------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
	
		PRINT '>> Inserting data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/cust_az12.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
	
		PRINT '>> Inserting data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/loc_a101.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT 'Loading Duration :' + CAST(DATEDIFF(second,@start_time,@end_time) AS VARCHAR) + 'seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v
	
		PRINT '>> Inserting data into : bronze.erp_px_cat_g1v';
		BULK INSERT bronze.erp_px_cat_g1v
		FROM '/Users/priyankamittal/Desktop/Product Analytics/sql-data-warehouse-project/datasets/source_crm/erp_px_cat_g1v2.csv'
		WITH (
			FIRSTROW =2
			FIELDTERMINATOR =','
			TABLOCK
		)
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
 EXEC bronze.load_bronze


--For Postgresql just right click on the table and then import the data with parameter
/*
Right-click your table bronze.crm_cust_info.
Select Import/Export Data.
Set:
Filename to your CSV path.
Format to CSV.
Header: Yes.
Delimiter: ,
Click OK.*/

---Check the quality of the table for ex: validate all column and data and check data is stored in correct column
SELECT * FROM bronze.crm_cust_info

SELECT COUNT(*) FROM bronze.crm_cust_info

SELECT * FROM bronze.crm_prd_info

SELECT COUNT(*) FROM bronze.crm_prd_info

TRUNCATE TABLE bronze.crm_sales_details
SELECT * FROM bronze.crm_sales_details
SELECT COUNT(*) FROM bronze.crm_sales_details

SELECT * FROM bronze.erp_cust_az12
SELECT COUNT(*) FROM bronze.erp_cust_az12

SELECT * FROM bronze.erp_loc_a101
SELECT COUNT(*) FROM bronze.erp_loc_a101

