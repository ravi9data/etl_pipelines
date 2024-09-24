DROP TABLE IF EXISTS hightouch_sources.catman_trackers_pending_allocation;
CREATE TABLE hightouch_sources.catman_trackers_pending_allocation AS  
WITH a AS (
			SELECT variant_sku,
			product_sku,
			store_name,
			customer_type,
			count(*) AS pending_allocations
			FROM 
			ods_production.subscription s 
			LEFT JOIN ods_production.customer c on s.customer_id = c.customer_id
			WHERE status = 'ACTIVE'
			AND allocation_status = 'PENDING ALLOCATION'
			AND variant_sku IS NOT NULL
			GROUP BY 1,2,3,4)
			, b AS (
			SELECT variant_sku, 
					product_sku,
					requested_total::int AS requested_per_variant,
					assets_stock_total::int AS stock_per_variant
					FROM
					ods_production.purchase_request
					)
			SELECT 
				a.*,
				b.requested_per_variant,
				b.stock_per_variant
				FROM a
				LEFT JOIN b ON (a.variant_sku = b.variant_sku AND a.product_sku = b.product_sku);
GRANT SELECT ON hightouch_sources.catman_trackers_pending_allocation TO hightouch;
GRANT SELECT ON hightouch_sources.catman_trackers_pending_allocation TO group pricing;
GRANT SELECT ON hightouch_sources.catman_trackers_pending_allocation TO hightouch_pricing;