/*
=====================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
Truncates the bronze tables before loading data.
Uses the  LOAD DATA LOCAL INFILE command to load data from csv Files to bronze tables.

Parameter:
  None.
  This stored procedure does not accept any parameters or return any values.
=====================================================================
*/
SET global local_infile = 1;

	Truncate Table bronze.crm_cust_info;
	LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
	INTO TABLE bronze.crm_cust_info
		FIELDS TERMINATED BY ',' 
		ENCLOSED BY '"'
		LINES TERMINATED BY '\r\n' 
		IGNORE 1 ROWS            
	(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, @var_cst_create_date)
	SET cst_create_date = STR_TO_DATE(replace(Trim(@var_cst_create_date), '\r', ''), '%d-%m-%Y');

	Truncate Table bronze.crm_prd_info;
	Load Data Local Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
		Into Table bronze.crm_prd_info
		fields Terminated By ','
		Enclosed by '"'
		Lines Terminated by '\r\n'
		Ignore 1 rows
	(prd_id, prd_key, prd_nm, prd_cost, prd_line,@var_prd_start_dt, @var_prd_end_dt)
	Set prd_start_dt = str_to_date(replace(Trim(@var_prd_start_dt), '\r', ''), '%d-%m-%Y'),
	prd_end_dt = str_to_date(replace(trim(@var_prd_end_dt), '\r', ''), '%d-%m-%Y');

	Truncate Table bronze.crm_sales_details;
	Load Data Local Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
		Into Table bronze.crm_sales_details
		fields terminated by ','
		enclosed by '"'
		lines Terminated by '\r\n'
		Ignore 1 rows;

	Truncate Table bronze.erp_cust_az12;
	Load Data Local Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CUST_AZ12.csv'
		Into table bronze.erp_cust_az12
		fields terminated by ','
		enclosed by'"'
		lines terminated by '\r\n'
		ignore 1 rows
	(cid,@var_bdate,gen)
	Set
	bdate = str_to_date(replace(trim(@var_bdate), '\r', ''), '%d-%m-%Y');

	Truncate Table bronze.erp_loc_a101;
	Load Data Local Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/LOC_A101.csv'
		Into Table bronze.erp_loc_a101
		fields terminated by ','
		enclosed by '"'
		lines terminated by '\r\n'
		ignore 1 rows;

	Truncate Table bronze.erp_px_cat_g1v2;
	Load Data Local Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PX_CAT_G1V2.csv'
		Into Table bronze.erp_px_cat_g1v2
		Fields terminated by ','
		enclosed by '"'
		Lines terminated by '\r\n'
		ignore 1 rows;

