DROP TABLE IF EXISTS tmp_supplier_stock_levels;
CREATE TEMP TABLE tmp_supplier_stock_levels
AS 
WITH base AS (
	SELECT
	  current_date AS fact_date,
	  a.asset_id AS asset_id,
	  a.warehouse AS warehouse,
	  CAST(a.supplier AS TEXT) AS supplier,
	  CAST(a.ean AS TEXT) AS ean,
	  CAST(a.category_name AS TEXT) AS category_name,
	  CAST(a.subcategory_name AS TEXT) AS subcategory_name,
	  CAST(a.brand AS TEXT) AS brand,
	  a.product_sku AS product_sku,
	  CAST(a.variant_sku AS TEXT) AS variant_sku,
	  CAST(a.product_name AS TEXT) AS product_name,
	  a.purchased_date AS purchased_date,
	  a.asset_condition,
	  a.asset_status_original,
	  a.serial_number,
	  pri.purchase_order_number
	FROM master.asset a
	LEFT JOIN ods_production.purchase_request_item pri 
		ON a.purchase_request_item_id = pri.purchase_request_item_id
	WHERE asset_status_original IN ('IN STOCK','INBOUND UNALLOCABLE')
)
SELECT 
	b.fact_date,
	b.supplier,
	b.warehouse,
	b.category_name,
	b.subcategory_name,
	b.brand,
	b.product_sku,
	b.variant_sku,
	b.ean,
	b.asset_condition,
	b.asset_status_original,
	b.serial_number,
	b.purchase_order_number,
	count(DISTINCT asset_id) AS in_stock_assets
FROM base b
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.supplier_stock_levels
WHERE fact_date = current_date::date
	OR (fact_date >= date_trunc('year',current_date)::date 
		AND date_part('dayofweek',fact_date) <> 1
		AND fact_date < dateadd('month',-3,current_date))
;

INSERT INTO dm_commercial.supplier_stock_levels
SELECT * 
FROM tmp_supplier_stock_levels;

END TRANSACTION;

GRANT SELECT ON dm_commercial.supplier_stock_levels TO tableau;
