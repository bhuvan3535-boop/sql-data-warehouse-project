-- for crm_cust_info
	-- check for duplicates and null values.
	Select
		cst_id,
		count(*)
	from Silver.crm_cust_info
	group by cst_id
	having count(*) > 1 or cst_id is null;

	-- Check for unwanted Spaces
	Select
		cst_create_date
	From silver.crm_cust_info
	Where cst_create_date != trim(cst_create_date);

	-- Data Standarization and consistency.
	Select distinct cst_marital_status
	From bronze.crm_cust_info;

	Select distinct cst_gndr
	from bronze.crm_cust_info;

-- for crm_prd_info
	-- check for duplicates and null values.
	Select
		prd_id,
		count(*)
	from silver.crm_prd_info
	group by prd_id
	having count(*) > 1 or prd_id is null;

	-- check for unwanted spaces
	Select prd_nm,
	 Trim(prd_nm)
	 from silver.crm_prd_info where 
	 Trim(prd_nm) != prd_nm;
	 -- check for Null or negative numbers
	 Select
		prd_cost
	from silver.crm_prd_info
	where prd_cost is null or prd_cost < 0;

	-- Data Standarization and consistency.
	Select distinct prd_line
	from silver.crm_prd_info; 

	Select *
	from silver.crm_prd_info
	where prd_start_dt > prd_end_dt;

-- for crm_sales_details
	-- check for invalid dates
    select 
		nullif (sls_order_dt, 0) as sls_order_dt
    from bronze.crm_sales_details
    where sls_order_dt <= 0 or length( sls_order_dt)!=8 or sls_order_dt >20500101;
    
    Select
		sls_due_dt
	from bronze.crm_sales_details
    where sls_due_dt <= 0 or 
		length( sls_due_dt)!=8 or 
		sls_due_dt >20500101 or
		sls_due_dt < sls_order_dt; 
        
	Select
		sls_sales as old_sales,
        sls_quantity,
        sls_price as old_price,
        Case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * nullif(abs(sls_price),0)
			then sls_quantity * abs(sls_price)
            else sls_sales
		End as sls_sales,
        Case when sls_price is null or sls_price <= 0 
			Then round(sls_sales/nullif(sls_quantity,0),0)
            else round(sls_price, 0)
		end as sls_price
	From bronze.crm_sales_details
    where sls_sales != sls_quantity*sls_price
    or sls_sales is null or sls_quantity is null or sls_price is null
    or sls_sales <= 0 or sls_quantity <=0 or sls_price <= 0;
    
-- for erp_cust_az12
Select 
    cid,
    bdate,
    gen
from silver.erp_cust_az12;
	Select bdate
	from silver.erp_cust_az12
	where bdate > now();

Select distinct  
cntry
from bronze.erp_loc_a101;

Select distinct cid from silver.erp_loc_a101
where cid not in (select cst_key from silver.crm_cust_info);

Select * from silver.crm_prd_info;
