/*
==========================================================================================
Stored Procedure:
Load Silver Layer (Bronze -> Silver)
==========================================================================================

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
-Truncates Silver tables.
Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
==========================================================================================
*/

-- script to Build customer info in silver layer.
Truncate Table silver.crm_cust_info;
Insert Into silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	Select 
		cst_id,
		cst_key,
		Trim(cst_firstname) as cst_firstname,
		Trim(cst_lastname) as cst_lastname,
		Case when upper(cst_marital_status) = 'S' Then 'Single'
			 When Upper(cst_marital_status) = 'M' Then 'Married'
			 Else 'Unknown'
			End as cst_marital_status,
		Case when upper(cst_gndr) = 'F' Then 'Female'
			 when upper(cst_gndr) = 'M' then 'Male'
			 else 'Unknown'
			End as cst_gndr,
		cst_create_date
	from(
			Select 
				*,
				row_number() over(partition by cst_id order by cst_create_date DESC) as Flag_last
			from bronze.crm_cust_info
			where cst_create_date is not Null
	)t where Flag_last = 1;

-- Script to build product info in silver layer
Truncate Table Silver.crm_prd_info;
insert into silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    Select
		prd_id,
        replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
        substring(prd_key, 7, length(prd_key)) as prd_key,
        prd_nm,
        coalesce(prd_cost, 0)  as prd_cost,
        Case when upper(prd_line) = 'R' Then 'Road'
			 when upper(prd_line) = 'S' Then 'other Sales'
             when upper(prd_line) = 'M' Then 'Mountain'
             when upper(prd_line) = 'T' then 'Touring'
             else 'Unknown'
			end as prd_line,
        cast(prd_start_dt as Date) as prd_stat_dt,
        date_sub(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt), Interval 1 day) as prd_end_dt
    From bronze.crm_prd_info;
    
    
-- Script to load crm_sales_details
Truncate Table Silver.crm_sales_details;
Insert into silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    Select
		sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        Case when sls_order_dt <=0 or length(sls_order_dt) != 8 then Null
			else cast(sls_order_dt as date)
			end as sls_order_dt,
        Case when sls_ship_dt <=0 or length(sls_ship_dt) != 8 then Null
			else cast(sls_ship_dt as date)
			end as sls_ship_dt,
        Case when sls_due_dt <=0 or length(sls_due_dt) != 8 then Null
			else cast(sls_due_dt as date)
			end as sls_due_dt,
        Case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * nullif(abs(sls_price),0)
			then sls_quantity * abs(sls_price)
            else sls_sales
			End as sls_sales,
		sls_quantity,
        Case when sls_price is null or sls_price <= 0 
			Then round(sls_sales/nullif(sls_quantity,0),0)
            else round(sls_price, 0)
			end as sls_price
	from bronze.crm_sales_details;

-- Script to load erp_cust_az12
Truncate Table Silver.erp_cust_az12;
Insert Into silver.erp_cust_az12 (cid, bdate, gen)
	Select 
		case when cid like 'NAS%' then substring(cid, 4, length(cid))
			else cid
			end as cid,
		case when bdate > now() then null
			else bdate
			end as badte,
		case when upper(trim(gen)) in ('F','female') then 'Female'
			when upper(trim(gen)) in ('M', 'Male') then 'Male'
            else 'Unknown'
            end gen
	from bronze.erp_cust_az12;

-- Script to load erp_loc_a101
Truncate Table Silver.erp_loc_a101;
Insert into silver.erp_loc_a101 (cid, cntry)
	Select
		replace(cid,'-','') as cid,
        case when upper(trim(cntry)) in ('us', 'united states','Usa') then 'United States'
			when upper(trim(cntry)) in ('DE', 'germany') then 'Germany'
            when cntry is Null or cntry = '' then 'unknown'
            else cntry
            end as cntry
	from bronze.erp_loc_a101;
    
-- Script to load erp_px_cat_g1v2
Truncate Table Silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	Select
		id,
        cat,
        subcat,
        maintenance
	from bronze.erp_px_cat_g1v2;
