/*
===================================================================================================================================
Stored Procedure: Load Bronze Layer  (Source -> Bronze)
===================================================================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external csv files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Eamples:
    EXEC Bronze.Load_bronze;

===================================================================================================================================
*/




CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,  @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY

			PRINT '==========================================================================================================';
			PRINT 'Loading Bronze Layer';
			PRINT '==========================================================================================================';
			-- Delete the content of the table before doing a full load
			PRINT '==========================================================================================================';
			PRINT 'Loading CRM Table';
			PRINT '==========================================================================================================';
			
			SET @batch_start_time = GETDATE();
			SET @start_time = GETDATE();
			PRINT '>> Truncating Table:bronze.crm_cust_info';

			TRUNCATE table bronze.crm_cust_info;

			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\cust_info.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			
			SET @end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR bronze.crm_cust_info : ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';

			PRINT '************************************************************************************************************************'
	
			-- Delete the content of the table before doing a full load
			PRINT '>> Truncating Table:bronze.crm_prd_info';
			SET @start_time = GETDATE();
			TRUNCATE table bronze.crm_prd_info;

			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\prd_info.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);

			SET @end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR bronze.crm_prd_info: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';

			PRINT '************************************************************************************************************************'

		

			-- Delete the content of the table before doing a full load
			PRINT '>> Truncating Table:bronze.crm_sales_details';
			SET @start_time = GETDATE();	
			TRUNCATE table bronze.crm_sales_details;


			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_details
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\sales_details.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR bronze.crm_sales_details: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';

			PRINT '************************************************************************************************************************'

			
			PRINT '==========================================================================================================';
			PRINT 'Loading ERP Table';
			PRINT '==========================================================================================================';

			-- Delete the content of the table before doing a full load
			PRINT '>> Truncating Table:bronze.erp_cust_az12';
			SET @start_time = GETDATE();
			TRUNCATE table bronze.erp_cust_az12;


			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\cust_az12.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR bronze.erp_cust_az12: ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';

			PRINT '************************************************************************************************************************'

		

			-- Delete the content of the table before doing a full load
			PRINT '>> Truncating Table:bronze.erp_LOC_A101';
			SET @start_time = GETDATE();
			TRUNCATE table bronze.erp_LOC_A101;


			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.erp_LOC_A101';
			BULK INSERT bronze.erp_LOC_A101
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\LOC_A101.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			SET @end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR bronze.erp_LOC_A101 : ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';

			PRINT '************************************************************************************************************************'

		
			-- Delete the content of the table before doing a full load
			PRINT '>> Truncating Table:bronze.erp_PX_CAT_G1V2';
			SET @start_time = GETDATE();
			TRUNCATE table bronze.erp_PX_CAT_G1V2;


			-- Bulk Insert
			PRINT '>> Inserting Data Into:bronze.erp_LOC_A101';
			BULK INSERT bronze.erp_PX_CAT_G1V2
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\PX_CAT_G1V2.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
			
				);
			SET @end_time = GETDATE();
			SET @batch_end_time = GETDATE();
			PRINT '>>> LOAD DURATION FOR PX_CAT_G1V2.csv : ' + cast(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + '  seconds';
			PRINT '>>> LOAD DURATION FOR Bronze Load Layer : ' + cast(DATEDIFF(second,@Batch_start_time,@batch_end_time) as NVARCHAR) + '  seconds';
			PRINT '************************************************************************************************************************'
			PRINT '==========================================================================================================';
			PRINT 'Operation Completed Successfully';
			PRINT '==========================================================================================================';
	END TRY -- ERROR HANDLING
	BEGIN CATCH
			PRINT '==========================================================================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message ' + ERROR_MESSAGE();
			PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '==========================================================================================================';
	END CATCH
END