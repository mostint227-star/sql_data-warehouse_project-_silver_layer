# sql_data-warehouse_project-_silver_layer
Building a Modern data warehouse with SQL server,including ETL processes, Data Modeling,and analytics.
------------------------------------------------------------------------------------------------------:
create or alter procedure silver.load_silver
as begin
print'>>truncating table:silver_crm_cust_info';
truncate table silver._crm_cust_ifo;
print'>>inserting data into:silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_first_name,
    cst_last_name,
    cst_marital_status,
    cst_gender,
    cst_create_date
)
SELECT 
    cst_id,
    TRIM(cst_key) AS cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) IN ('M', 'MARRIED') THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) IN ('S', 'SINGLE') THEN 'Single'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS rn
    FROM BRONZE.crm_cust_info
) t
end
