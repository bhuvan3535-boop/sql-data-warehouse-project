/*
======================================================================
DDL Script: Create Gold Views
======================================================================

Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)
Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.
Usage:
These views can be queried directly for analytics and reporting.
======================================================================

*/
Drop view gold.dim_customers;
Create view gold.dim_customers as 
	Select
		row_number() over(order by ci.cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name, 
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		Case when ci.cst_gndr != 'unknown' Then ci.cst_gndr
			Else coalesce(ca.gen, 'unknown')
			end as gender,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date
	From silver.crm_cust_info as ci
	Left Join silver.erp_cust_az12 as ca
	on ci.cst_key = ca.cid
	Left Join silver.erp_loc_a101 as la
	on ci.cst_key = la.cid;
======================================================================


Drop view gold.dim_products;
Create view gold.dim_products as    
	Select
		row_number() over(order by pd.prd_start_dt, pd.prd_key) as product_key,
		pd.prd_id as product_id,
		pd.prd_key as product_number,
		pd.prd_nm as product_name,
		pd.cat_id as category_id,
		px.cat as category,
		px.subcat as subcategory,
		px.maintenance,
		pd.prd_cost as product_cost,
		pd.prd_line as product_line,
		pd.prd_start_dt as start_date
	from silver.crm_prd_info as pd
	Left Join silver.erp_px_cat_g1v2 as px
	on pd.cat_id = px.id
	where pd.prd_end_dt is null; -- filter out historiacal data
======================================================================


Drop view gold.fact_sales;
create view gold.fact_sales as    
	Select
		sls_ord_num as order_number,
		pr.product_key,
		cu.customer_key,
		sls_order_dt as order_date,
		sls_ship_dt as shipping_date,
		sls_due_dt as due_date,
		sls_sales as sales_amount,
		sls_quantity as quantity,
		sls_price as price	
	from silver.crm_sales_details as sd
	Left join gold.dim_products as pr
	on sd.sls_prd_key = pr.product_number
	left join gold.dim_customers as cu
	on sd.sls_cust_id = cu.customer_id;
