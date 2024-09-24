	
--Dashboard base table for purchase discount report
-- End User  CatMan Team

--drop table if exists dm_commercial.purchase_discount_report;
--create table dm_commercial.purchase_discount_report as


DELETE FROM dm_commercial.purchase_discount_report WHERE ah_date = current_date;

INSERT INTO dm_commercial.purchase_discount_report
	WITH a AS 
	(SELECT
	  current_date AS ah_date,
	  a.asset_id AS asset_id,
	  a.customer_id AS customer_id,
	  a.subscription_id AS subscription_id,
	  a.created_at AS created_at,
	  a.updated_at AS updated_at,
	  al.allocated_at AS allocated_at,
	  a.asset_allocation_id AS asset_allocation_id,
	  a.asset_allocation_sf_id AS asset_allocation_sf_id,
	  a.warehouse AS warehouse,
	  a.capital_source_name AS capital_source_name,
	  CAST(a.supplier AS TEXT) AS supplier,
	  a.first_allocation_store AS first_allocation_store,
	  a.first_allocation_store_name AS first_allocation_store_name,
	  a.first_allocation_customer_type AS first_allocation_customer_type,
	  a.serial_number AS serial_number,
	  CAST(a.ean AS TEXT) AS ean,
	  a.product_sku AS product_sku,
	  a.asset_name AS asset_name,
	  CAST(a.asset_condition AS TEXT) AS asset_condition,
	  a.asset_condition_spv AS asset_condition_spv,
	  CAST(a.variant_sku AS TEXT) AS variant_sku,
	  CAST(a.product_name AS TEXT) AS product_name,
	  CAST(a.category_name AS TEXT) AS category_name,
	  CAST(a.subcategory_name AS TEXT) AS subcategory_name,
	  CAST(a.brand AS TEXT) AS brand,
	  a.invoice_url AS invoice_url,
	  a.total_allocations_per_asset AS total_allocations_per_asset,
	  a.purchased_date AS purchased_date,
	  a.months_since_purchase AS months_since_purchase,
	  a.days_since_purchase AS days_since_purchase,
	  a.amount_rrp AS amount_rrp,
	  a.initial_price AS initial_price,
	  a.residual_value_market_price AS residual_value_market_price,
	  a.last_month_residual_value_market_price AS last_month_residual_value_market_price,
	  a.average_of_sources_on_condition_this_month AS average_of_sources_on_condition_this_month,
	  a.average_of_sources_on_condition_last_available_price AS average_of_sources_on_condition_last_available_price,
	  a.sold_price AS sold_price,
	  a.sold_date AS sold_date,
	  a.currency AS currency,
	  a.asset_status_original AS asset_status_original,
	  a.asset_status_new AS asset_status_new,
	  a.asset_status_detailed AS asset_status_detailed,
	  a.lost_reason AS lost_reason,
	  a.last_allocation_days_in_stock AS last_allocation_days_in_stock,
	  a.last_allocation_dpd AS last_allocation_dpd,
	  a.dpd_bucket AS dpd_bucket,
	  a.subscription_revenue AS subscription_revenue,
	  a.subscription_revenue_due AS subscription_revenue_due,
	  a.subscription_revenue_last_31day AS subscription_revenue_last_31day,
	  a.subscription_revenue_last_month AS subscription_revenue_last_month,
	  a.subscription_revenue_current_month AS subscription_revenue_current_month,
	  a.avg_subscription_amount AS avg_subscription_amount,
	  a.max_subscription_amount AS max_subscription_amount,
	  a.payments_due AS payments_due,
	  a.last_payment_amount_due AS last_payment_amount_due,
	  a.last_payment_amount_paid AS last_payment_amount_paid,
	  a.payments_paid AS payments_paid,
	  a.shipment_cost_paid AS shipment_cost_paid,
	  a.repair_cost_paid AS repair_cost_paid,
	  a.customer_bought_paid AS customer_bought_paid,
	  a.additional_charge_paid AS additional_charge_paid,
	  a.delivered_allocations AS delivered_allocations,
	  a.returned_allocations AS returned_allocations,
	  a.max_paid_date AS max_paid_date,
	  a.office_or_sponsorships AS office_or_sponsorships,
	  a.last_market_valuation AS last_market_valuation,
	  a.last_valuation_report_date AS last_valuation_report_date,
	  a.asset_value_linear_depr AS asset_value_linear_depr,
	  a.market_price_at_purchase_date AS market_price_at_purchase_date,
	  a.shipping_country AS shipping_country,
	  CAST(a.asset_sold_invoice AS TEXT) AS asset_sold_invoice,
	  a.invoice_date AS invoice_date,
	  a.invoice_number AS invoice_number,
	  a.invoice_total AS invoice_total,
	  a.revenue_share AS revenue_share,
	  a.first_order_id AS first_order_id,
	  a.country AS country,
	  a.city AS city,
	  a.postal_code AS postal_code,
	  current_date AS date,
	  current_date AS snapshot_time,
	  a.purchase_price_commercial AS purchase_price_commercial,
	  CASE 
			WHEN pri.purchase_order_number LIKE '%-PO-%' THEN 'Regular Stock'
			WHEN pri.purchase_order_number LIKE '%-BS-%' THEN 'B-Stock'
			WHEN pri.purchase_order_number LIKE '%-OB-%' THEN 'Open-box Stock'
			WHEN pri.purchase_order_number LIKE '%-BB-%' THEN 'B2B Specific'
			WHEN pri.purchase_order_number LIKE '%-OF-%' THEN 'Offline Partners Store'
			WHEN pri.purchase_order_number LIKE '%-PA-%' THEN 'MSD Orders Partners'
			WHEN pri.purchase_order_number LIKE '%-MA-%' THEN 'MediaMarkt Austria'
			WHEN pri.purchase_order_number LIKE '%-ME-%' THEN 'MediaMarkt Spain'
			WHEN pri.purchase_order_number LIKE '%-PR-%' THEN 'Others'
	  END AS stock_type
	FROM master.asset a
	LEFT JOIN master.allocation al ON al.allocation_id = a.asset_allocation_id 
	LEFT JOIN ods_production.purchase_request_item pri ON pri.purchase_request_item_id = a.purchase_request_item_id	
	WHERE current_date = LAST_DAY(current_date) --removed the 15th as requested, then will need to add new column AND populate past dates  
)
, b AS (
	SELECT 
		a.product_sku,
		a.ah_date,
		max(a.market_price_at_purchase_date) AS max_market_price
	FROM a
	GROUP BY 1,2
)
, c AS (
	SELECT 
		b.*,
		max(b1.max_market_price) AS max_market_price_per_sku
	FROM b 
	LEFT JOIN b b1
		ON b.product_sku = b1.product_sku
		 AND b1.ah_date >= dateadd('year',-1,b.ah_date)
	GROUP BY 1,2,3
)
SELECT 
	  a.ah_date,
	  a.asset_id,
      a.customer_id,
      a.subscription_id,
      a.created_at,
      a.updated_at,
      a.allocated_at,
      a.asset_allocation_id,
      a.asset_allocation_sf_id,
      a.warehouse,
      a.capital_source_name,
      a.supplier,
      a.first_allocation_store,
      a.first_allocation_store_name,
      a.first_allocation_customer_type,
      a.serial_number,
      a.ean,
      a.product_sku,
      a.asset_name,
      a.asset_condition,
      a.asset_condition_spv,
      a.variant_sku,
      a.product_name,
      a.category_name,
      a.subcategory_name,
      a.brand,
      a.invoice_url,
      a.total_allocations_per_asset,
      a.purchased_date,
      a.months_since_purchase,
      a.days_since_purchase,
      a.amount_rrp,
      a.initial_price,
      a.residual_value_market_price,
      a.last_month_residual_value_market_price,
      a.average_of_sources_on_condition_this_month,
      a.average_of_sources_on_condition_last_available_price,
      a.sold_price,
      a.sold_date,
      a.currency,
      a.asset_status_original,
      a.asset_status_new,
      a.asset_status_detailed,
      a.lost_reason,
      a.last_allocation_days_in_stock,
      a.last_allocation_dpd,
      a.dpd_bucket,
      a.subscription_revenue,
      a.subscription_revenue_due,
      a.subscription_revenue_last_31day,
      a.subscription_revenue_last_month,
      a.subscription_revenue_current_month,
      a.avg_subscription_amount,
      a.max_subscription_amount,
      a.payments_due,
      a.last_payment_amount_due,
      a.last_payment_amount_paid,
      a.payments_paid,
      a.shipment_cost_paid,
      a.repair_cost_paid,
      a.customer_bought_paid,
      a.additional_charge_paid,
      a.delivered_allocations,
      a.returned_allocations,
      a.max_paid_date,
      a.office_or_sponsorships,
      a.last_market_valuation,
      a.last_valuation_report_date,
      a.asset_value_linear_depr,
      a.market_price_at_purchase_date,
      a.shipping_country,
      a.asset_sold_invoice,
      a.invoice_date,
      a.invoice_number,
      a.invoice_total,
      a.revenue_share,
      a.first_order_id,
      a.country,
      a.city,
      a.postal_code,
      a.date,
      a.snapshot_time,
      a.purchase_price_commercial,
	  c.max_market_price_per_sku,
	  a.stock_type
FROM a
LEFT JOIN c
	ON a.product_sku = c.product_sku
	 AND a.ah_date = c.ah_date
;

GRANT SELECT ON dm_commercial.purchase_discount_report TO tableau;
