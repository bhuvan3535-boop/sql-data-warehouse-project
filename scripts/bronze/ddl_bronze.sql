/*
=====================================================================
DDL Script: Create Bronze Tables
=====================================================================
Script purpose:
    This Script creates Table in the 'Broze' schema, dropping the exiting tables
    if they  already exist.
    Run this script redefine the DDL Structue of the 'Bronze' Tables
=====================================================================
*/

Drop Table IF EXISTS bronze.crm_cust_info;
Create Table bronze.crm_cust_info(
	cst_id 				INT,
    cst_key 			NVARCHAR(50),
    cst_firstname 		NVARCHAR(50),
    cst_lastname 		NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr 			NVARCHAR(50),
    cst_create_date 	DATE
);

DROP Table IF exists bronze.crm_prd_info;
Create Table bronze.crm_prd_info(
	prd_id 			INT,
    prd_key 		NVARCHAR(50),
    prd_nm 			NVARCHAR(50),
    prd_cost 		INT,
    prd_line 		NVARCHAR(50),
    prd_start_dt 	DATE,
    prd_end_dt 		DATE
);

DROP Table IF EXISTS bronze.crm_sales_details;
Create Table bronze.crm_sales_details(
	sls_ord_num 	NVARCHAR(50),
    sls_prd_key 	NVARCHAR(50),
    sls_cust_id 	INT,
    sls_order_dt 	INT,
    sls_ship_dt 	INT,
    sls_due_dt 		INT,
    sls_sales 		INT,
    sls_quantity 	INT,
    sls_price 		INT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12(
	cid 	NVARCHAR(50),
    bdate 	DATE,
    gen 	NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
Create Table bronze.erp_loc_a101(
	cid 	NVARCHAR(50),
    cntry 	NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2(
	id 			NVARCHAR(50),
    cat 		NVARCHAR(50),
    subcat 		NVARCHAR(50),
    maintenance NVARCHAR(50)
);
