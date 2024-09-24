CREATE OR REPLACE VIEW dm_commercial.v_assets_back_to_in_stock_status AS 
WITH prep_weekly AS (
SELECT
	date_trunc('week',ah.createddate)::date AS fact_date,
	date_part('year',a.purchased_date) AS year_of_purchase,
	ah.assetid, 
	ah.oldvalue,
	a.category_name,
	a.subcategory_name,
	a.brand,
	a.product_sku,
	ROW_NUMBER () OVER (PARTITION BY assetid,fact_date ORDER BY fact_date DESC) AS rowno
FROM stg_salesforce.asset_history ah 
LEFT JOIN master.asset a 
	ON a.asset_id = ah.assetid 
WHERE ah.field = 'Status'
	AND ah.newvalue = 'IN STOCK'
)
, prep_monthly AS (
SELECT
	date_trunc('month',ah.createddate)::date AS fact_date,
	date_part('year',a.purchased_date) AS year_of_purchase,
	ah.assetid, 
	ah.oldvalue,
	a.category_name,
	a.subcategory_name,
	a.brand,
	a.product_sku,
	ROW_NUMBER () OVER (PARTITION BY assetid,fact_date ORDER BY fact_date DESC) AS rowno
FROM stg_salesforce.asset_history ah 
LEFT JOIN master.asset a 
	ON a.asset_id = ah.assetid 
WHERE ah.field = 'Status'
	AND ah.newvalue = 'IN STOCK'
)
SELECT 
	'Weekly' AS reporting_period,
	w.fact_date,
	w.year_of_purchase,
	w.oldvalue,
	w.category_name,
	w.subcategory_name,
	w.brand,
	w.product_sku,
	count(DISTINCT assetid) AS assets_back_in_stock
FROM prep_weekly w
WHERE w.rowno = 1
GROUP BY 1,2,3,4,5,6,7,8
UNION 
SELECT 
	'Monthly' AS reporting_period,
	m.fact_date,
	m.year_of_purchase,
	m.oldvalue,
	m.category_name,
	m.subcategory_name,
	m.brand,
	m.product_sku,
	count(DISTINCT assetid) AS assets_back_in_stock
FROM prep_monthly m
WHERE m.rowno = 1
GROUP BY 1,2,3,4,5,6,7,8
WITH NO SCHEMA BINDING
;
