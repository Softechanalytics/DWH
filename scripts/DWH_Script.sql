use master;
use Data_warehouse;
Create Schema Gold;
Go
Create Schema Silver;
Go
Create Schema Bronze;
Go


IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info ;
create table bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date Date
);
GO

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info ;
create table bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt Date,
prd_end_dt Date
);
GO

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details ;
create table bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
GO

IF OBJECT_ID ('bronze.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12 ;
create table bronze.erp_CUST_AZ12 (
cid NVARCHAR(50),
bdate Date,
gen NVARCHAR(10)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101 ;
create table bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2 ;
create table bronze.erp_PX_CAT_G1V2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(5)
);
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.crm_cust_info;
			-- Bulk Insert

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
			

			


			-- Delete the content of the table before doing a full load

			TRUNCATE table bronze.erp_cust_az12;


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

			select top 10*
			from  bronze.erp_PX_CAT_G1V2;

			select count(*)
			