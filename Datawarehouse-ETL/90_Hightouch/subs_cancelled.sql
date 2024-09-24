DROP TABLE IF EXISTS hightouch_sources.catman_trackers_subs_cancelled;
CREATE TABLE hightouch_sources.catman_trackers_subs_cancelled AS 
WITH cancellation_info AS (
	SELECT
		a2.asset_id,
		a2.serial_number, 
		s.variant_sku,
		s.product_sku,
		s.store_name,
		s.subscription_value AS rental_plan_price,
		s.cancellation_date::date,
		a.cancellation_returned_at::date AS return_scanned_date,
		a.refurbishment_end_at::date AS refurbishment_end_date,
		cr.cancellation_reason_new
	FROM ods_production.subscription s
	LEFT JOIN ods_production.subscription_cancellation_reason cr 
		ON s.subscription_id = cr.subscription_id
	LEFT JOIN ods_production.allocation a 
		ON a.subscription_id = s.subscription_id
	LEFT JOIN master.asset a2 
		ON a2.asset_id = a.asset_id
)
, back_in_stock_raw AS (
	SELECT 
		assetid AS asset_id,
		createddate AS in_stock_date,
		field,
		oldvalue,
		newvalue
	FROM stg_salesforce.asset_history
	WHERE field = 'Status'
		AND newvalue = 'IN STOCK'
)
, back_in_stock AS (
	SELECT 
		c.asset_id,
		c.serial_number,
		c.variant_sku,
		c.product_sku,
		c.store_name,
		c.rental_plan_price,
		c.cancellation_date,
		c.return_scanned_date,
		c.refurbishment_end_date,
		c.cancellation_reason_new,
		b.in_stock_date::date,
		ROW_NUMBER() OVER (PARTITION BY c.asset_id ORDER BY in_stock_date ASC) AS rowno
	FROM cancellation_info c
	LEFT JOIN back_in_stock_raw b
		ON b.asset_id = c.asset_id
		 AND b.in_stock_date > c.cancellation_date
	WHERE c.cancellation_date >= CURRENT_DATE-7*7
)
SELECT 
	asset_id,
	serial_number,
	variant_sku,
	product_sku,
	store_name,
	rental_plan_price,
	cancellation_date,
	return_scanned_date,
	refurbishment_end_date,
	cancellation_reason_new,
	in_stock_date
FROM back_in_stock
WHERE rowno = 1
;

GRANT SELECT ON hightouch_sources.catman_trackers_subs_cancelled TO hightouch;
GRANT SELECT ON hightouch_sources.catman_trackers_subs_cancelled TO group pricing;
GRANT SELECT ON hightouch_sources.catman_trackers_subs_cancelled TO hightouch_pricing;