-- Check for Nulls or Duplicates in Primary key
-- Expectation: No Result

Select 
        cst_id,
        count(*)
From bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL


-- Note 6 duplicated were identified with  3 Null cst_id key

            Select *
            From bronze.crm_cust_info
            where cst_id = 29466;


Select 
        *
        From(
            Select 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
            From bronze.crm_cust_info)t 
        where flag_last = 1;


-- Check for unwanted space
-- Expectation: No Result
Select cst_firstname
From bronze.crm_cust_info 
where cst_firstname != trim(cst_firstname);

Select cst_lastname
From bronze.crm_cust_info 
where cst_lastname != trim(cst_lastname);


Select cst_material_status
From bronze.crm_cust_info 
where cst_material_status != trim(cst_material_status);

Select cst_gndr
From bronze.crm_cust_info 
where cst_gndr != trim(cst_gndr);
print '-----------------------------------------------------------------------------------';
/*
====================================================================================================================
Purpose: Data cleansing and updating the silver.crm_cust_info Table
Result: No Duplicate or Abbreviation expected.
====================================================================================================================
*/

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)

Select 
cst_id,
cst_key,
cst_Firstname = trim(cst_firstname),
cst_lastname = trim(cst_lastname),
case when upper(TRIM(cst_material_status)) = 'M' Then 'Married'
     when upper(TRIM(cst_material_status)) = 'S' Then 'Single'
     ELSE 'n/a'
END cst_material_status,
case when upper(TRIM(cst_gndr)) = 'F' Then 'Female'
     when upper(TRIM(cst_gndr)) = 'M' Then 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date
 From(
            Select 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
            From bronze.crm_cust_info)t 
        where flag_last = 1;

----------

-- Data Standardization & Consistency

Select DISTINCT cst_gndr
From bronze.crm_cust_info;