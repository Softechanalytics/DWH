/*
=====================================================================================================================================
  DDL Script: Create silver Table
=====================================================================================================================================

Script Purpose:
    This script creates tables in the 'silver' Schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'silver' Tables.
=====================================================================================================================================
*/


IF OBJECT_ID ('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info ;
create table silver.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date Date,
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO

IF OBJECT_ID ('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info ;
create table silver.crm_prd_info (
prd_id INT,
Cat_ID NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt Date,
prd_end_dt Date,
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO

IF OBJECT_ID ('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details ;
create table silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO

IF OBJECT_ID ('silver.erp_CUST_AZ12','U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12 ;
create table silver.erp_CUST_AZ12 (
cid NVARCHAR(50),
bdate Date,
gen NVARCHAR(10),
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO

IF OBJECT_ID ('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101 ;
create table silver.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2 ;
create table silver.erp_PX_CAT_G1V2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(5),
dwh_create_date DATETIME2 DEFAULT GETDATE() -- Date created by data engineer for tracability
);
GO