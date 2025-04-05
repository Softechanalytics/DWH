SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE   PROCEDURE [Bronze].[load_bronze] AS
BEGIN
	
            TRUNCATE table bronze.crm_cust_info
			BULK INSERT bronze.crm_cust_info
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\cust_info.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			



			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.crm_prd_info;
			-- Bulk Insert

			BULK INSERT bronze.crm_prd_info
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\prd_info.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			

			select top 10*
			from bronze.crm_prd_info;

			select count(*)
			from bronze.crm_prd_info;




			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.crm_sales_details;


			-- Bulk Insert

			BULK INSERT bronze.crm_sales_details
			From 'D:\Portfolio_Website\DWH\datasets\source_crm\sales_details.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			

		




			-- Bulk Insert

			BULK INSERT bronze.erp_cust_az12
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\cust_az12.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			

			
			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.erp_LOC_A101;


			-- Bulk Insert

			BULK INSERT bronze.erp_LOC_A101
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\LOC_A101.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			

			

			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.erp_PX_CAT_G1V2;


			-- Bulk Insert
			BULK INSERT bronze.erp_PX_CAT_G1V2
			From 'D:\Portfolio_Website\DWH\datasets\source_erp\PX_CAT_G1V2.csv' -- location of where the file to be uploaded is stored
			WITH (
					FIRSTROW = 2,  -- THE UPLOAD SHOULD START FROM ROW LOW, AS WE ALREADY CREATED THE TABLE STRUCTURE
					FIELDTERMINATOR = ',', -- WE ARE SEPERATOR OF CSV FILE IS THE COMMA, DELIMINATOR
					TABLOCK
				);
			END


			-- Quality Check
			-- confirm the columns are copied correctly
			select top 10 *
			from bronze.crm_cust_info;

			-- count the rows
			select count(*)
			from bronze.crm_cust_info;

            		-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.crm_cust_info;
			-- Bulk Insert