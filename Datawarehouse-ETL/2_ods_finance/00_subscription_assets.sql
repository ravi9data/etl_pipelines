DROP TABLE IF EXISTS ods_production.subscription_assets;
CREATE TABLE ods_production.subscription_assets AS 
WITH purchase_price AS (
SELECT DISTINCT 
  asset_id
 ,asset_name
 ,COALESCE(initial_price,
           CASE
            WHEN revenue_share IS FALSE 
             THEN MAX(initial_price) OVER (PARTITION BY asset_name)
            ELSE NULL::DOUBLE PRECISION
           END) AS purchase_price
 ,COALESCE(amount_rrp,
           CASE
            WHEN revenue_share IS FALSE 
             THEN MAX(amount_rrp) OVER (PARTITION BY asset_name)
            ELSE NULL::DOUBLE PRECISION
           END) AS amount_rrp
FROM ods_production.asset 
)
,total_allocations AS (
SELECT 
  subscription_id
 ,COUNT(allocation_id) AS allocated_assets
FROM ods_production.allocation
GROUP BY 1 
)
,replacements AS (
SELECT 
  s.subscription_id
 ,COUNT(DISTINCT CASE 
   WHEN ta.allocated_assets > 1 
     AND a.allocation_status_original != 'TO BE FIXED' 
    THEN
     CASE 
      WHEN a.replaced_by IS NOT NULL 
       THEN a.allocation_id 
      WHEN a.replacement_date IS NOT NULL 
       THEN a.allocation_id
      WHEN a.replacement_for IS NOT NULL
       THEN a.replacement_for
      ELSE NULL 
     END 
   ELSE NULL
  END) AS replacement_assets
 ,SUM(a.days_on_rent) AS days_on_rent
 ,SUM(a.months_on_rent) AS months_on_rent			 		
FROM ods_production.subscription s		 		
  LEFT JOIN ods_production.allocation a 
    ON s.subscription_id = a.subscription_id
  LEFT JOIN total_allocations ta 
    ON ta.subscription_id = a.subscription_id
WHERE s.subscription_id IS NOT NULL
GROUP BY 1 
)
SELECT 
  aa.subscription_id
 ,ta.allocated_assets 
 ,LISTAGG(CASE 
   WHEN aa.delivered_at IS NOT NULL 
     AND aa.return_delivery_date IS NULL 
     AND aa.allocation_status_original <> 'RETURNED'
     AND aa.is_last_allocation_per_asset = TRUE
     AND a.asset_status_original NOT IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED') 
    THEN p.product_name 
   ELSE NULL 
  END, ' / '::TEXT) AS outstanding_product_names
 ,GREATEST(SUM(CASE 
   WHEN aa.delivered_at IS NOT NULL 
    THEN COALESCE(aaa.purchase_price, 0::DOUBLE PRECISION) 
   ELSE 0 
  END)
  - SUM(CASE 
   WHEN aa.return_delivery_date IS NOT NULL 
     OR aa.allocation_status_original = 'RETURNED'
     OR aa.is_last_allocation_per_asset IS FALSE 
     OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED') 
      AND aa.failed_delivery_at IS NULL)
    THEN COALESCE(aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0
  END), 0) AS outstanding_purchase_price
 ,GREATEST(SUM(CASE 
   WHEN aa.delivered_at IS NOT NULL 
    THEN COALESCE(aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0 
  END)
  -SUM(CASE 
   WHEN aa.return_delivery_date IS NOT NULL 
     OR aa.allocation_status_original = 'RETURNED'
     OR aa.is_last_allocation_per_asset IS FALSE 
     OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','OFFICE','RECOVERED','DELETED') AND aa.failed_delivery_at IS NULL)
    THEN COALESCE(aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0 
  END), 0) AS outstanding_purchase_price_with_lost
 ,GREATEST(SUM(CASE 
   WHEN aa.delivered_at IS NOT NULL 
     OR aa.is_last_allocation_per_asset IS FALSE
    THEN COALESCE(gmv.mkp,aaa.amount_rrp, aaa.purchase_price, 0::DOUBLE PRECISION) 
   ELSE 0 
  END)
  -SUM(CASE 
   WHEN aa.return_delivery_date IS NOT NULL 
     OR aa.allocation_status_original = 'RETURNED'
     OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED') AND aa.failed_delivery_at IS NULL)
    THEN COALESCE(gmv.mkp, aaa.amount_rrp, aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0 
  END), 0) AS outstanding_rrp
 ,GREATEST(SUM(CASE 
   WHEN aa.delivered_at IS NOT NULL 
     OR aa.is_last_allocation_per_asset IS FALSE
    THEN COALESCE(gmv.mkp, aaa.amount_rrp, aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0 
  END)
  -SUM(CASE 
   WHEN aa.return_delivery_date IS NOT NULL 
     OR aa.allocation_status_original = 'RETURNED'
     OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','OFFICE','RECOVERED','DELETED') 
      AND aa.failed_delivery_at IS NULL)
    THEN COALESCE(gmv.mkp, aaa.amount_rrp, aaa.purchase_price, 0::DOUBLE PRECISION)
   ELSE 0 
  END), 0) AS outstanding_rrp_with_lost
 ,COALESCE(LISTAGG(CASE 
   WHEN aa.delivered_at IS NOT NULL 
     AND aa.return_delivery_date IS NULL             
 			AND aa.allocation_status_original <> 'RETURNED'
      AND aa.is_last_allocation_per_asset = TRUE
      AND a.asset_status_original NOT IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED') 
    THEN aa.allocation_sf_id 
   ELSE NULL::CHARACTER VARYING
  END::TEXT, ' / '::TEXT), 'n/a'::TEXT) AS outstanding_allocation_ids 
 ,COALESCE(LISTAGG(CASE 
   WHEN aa.delivered_at IS NOT NULL
     AND aa.return_delivery_date IS NULL             
 		 AND aa.allocation_status_original <> 'RETURNED'
     AND aa.is_last_allocation_per_asset = TRUE
     AND a.asset_status_original NOT IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED')
    THEN aa.serial_number ELSE NULL::CHARACTER VARYING
   END::TEXT, ' / '::TEXT), 'n/a'::TEXT) AS outstanding_serial_numbers
 ,GREATEST(SUM(CASE 
   WHEN aa.delivered_at IS NOT NULL 
    THEN COALESCE(mv.final_price,0::DOUBLE PRECISION)
   ELSE 0 
  END)
  -SUM(CASE
  WHEN aa.return_delivery_date IS NOT NULL 
    OR aa.allocation_status_original = 'RETURNED'
    OR aa.is_last_allocation_per_asset IS FALSE 
    OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED')
      AND aa.failed_delivery_at IS NULL)
   THEN COALESCE(mv.final_price, 0::DOUBLE PRECISION) ELSE 0 
  END), 0) AS outstanding_residual_asset_value
 ,SUM(CASE 
   WHEN wrl.asset_status_original = 'WRITTEN OFF DC'
     AND aa.delivered_at IS NOT NULL
    THEN COALESCE(wrl.residual_value_market_price_written_off_and_lost, 0::DOUBLE PRECISION) 
   ELSE 0
  END) AS asset_valuation_written_off
 ,SUM(CASE
   WHEN wrl.asset_status_original = 'LOST'
     AND aa.delivered_at IS NOT NULL
    THEN COALESCE(wrl.residual_value_market_price_written_off_and_lost, 0::DOUBLE PRECISION) 
   ELSE 0 
  END) AS asset_valuation_lost
 ,MIN(aa.delivered_at)::DATE AS first_asset_delivered
 ,COUNT(CASE 
   WHEN aa.failed_delivery_at IS NOT NULL 
    THEN aa.allocation_id 
   END) AS failed_delivery_assets
 ,COUNT(CASE 
   WHEN aa.delivered_at IS NOT NULL
    THEN aa.allocation_id
  END) AS delivered_assets
 ,MAX(rr.replacement_assets) AS replacement_assets
 ,COUNT(CASE
   WHEN a.asset_status_original = 'IN DEBT COLLECTION'
     AND aa.is_last_allocation_per_asset IS TRUE 
    THEN aa.allocation_id
  END) AS debt_collection_assets
 ,MAX(LEAST(return_shipment_at, aa.return_delivery_date, return_processed_at)) AS last_return_shipment_at
 ,MAX(cancellation_returned_at)::DATE AS last_asset_returned
 ,COUNT(CASE
   WHEN cancellation_returned_at IS NOT NULL
    THEN aa.allocation_id 
  END) AS returned_assets
 ,COUNT(CASE
   WHEN aa.return_delivery_date IS NOT NULL
    THEN aa.allocation_id 
  END) AS returned_packages
 ,COUNT(CASE
   WHEN aa.return_delivery_date IS NULL AND return_shipment_at IS NOT NULL
    THEN aa.allocation_id 
  END ) AS in_transit_assets
 ,COUNT(CASE
   WHEN aa.delivered_at IS NULL
    THEN 
	 	 CASE 
      WHEN picked_by_carrier_at IS NOT NULL 
       THEN aa.allocation_id 
     END 
    WHEN aa.delivered_at IS NOT NULL 
      AND shipment_tracking_number IS NOT NULL 
     THEN aa.allocation_id 
   END) AS shipped_assets
 ,MAX(CASE 
   WHEN rr.replacement_assets > 0
    THEN rr.days_on_rent
   ELSE aa.days_on_rent
  END) AS days_on_rent
 ,MAX(CASE 
   WHEN rr.replacement_assets > 0
    THEN rr.months_on_rent
   ELSE aa.months_on_rent
  END) AS months_on_rent
 ,GREATEST(COUNT(CASE 
   WHEN aa.delivered_at IS NOT NULL 
    THEN aa.allocation_id 
  END)
  -COUNT(CASE 
   WHEN aa.delivered_at IS NOT NULL 
	   AND (aa.return_delivery_date IS NOT NULL 
       OR aa.allocation_status_original = 'RETURNED'
       OR aa.is_last_allocation_per_asset IS FALSE 
       OR (a.asset_status_original IN ('RETURNED TO SUPPLIER','SOLD','RETURNED','LOST','OFFICE','RECOVERED','DELETED') 
        AND aa.failed_delivery_at IS NULL))
    THEN aa.allocation_id 
  END), 0) AS outstanding_assets
 ,COUNT(CASE
   WHEN aa.returned_final_condition::TEXT = 'DAMAGED'::TEXT
    THEN aa.allocation_id
   ELSE NULL::CHARACTER VARYING
  END) AS damaged_allocations
 ,AVG(CASE 
   WHEN a.initial_price > 0 
    THEN a.initial_price 
  END) AS avg_asset_purchase_price
 ,MAX(CASE 
   WHEN aa.is_agreement IN ('Re-circulation agreement', 'Bundle Re-circulation') 
     AND aa.is_recirculated = 'Re-circulated'
    THEN 1 
   ELSE 0 
  END) AS is_revenue_share_agreement
 ,LISTAGG(DISTINCT aa.is_recirculated, '/') AS asset_recirculation_status
 ,MAX(GREATEST(aa.updated_at, a.updated_date, p.updated_at)) AS updated_at
FROM ods_production.allocation aa 
  LEFT JOIN total_allocations ta 
    ON ta.subscription_id = aa.subscription_id
  LEFT JOIN replacements rr 
    ON rr.subscription_id = aa.subscription_id
  LEFT JOIN ods_production.asset a 
    ON aa.asset_id = a.asset_id
  LEFT JOIN ods_production.product p 
    ON a.product_sku = p.product_sku
  LEFT JOIN purchase_price aaa 
    ON aaa.asset_id::TEXT = a.asset_id::TEXT
  LEFT JOIN ods_production.spv_report_master mv 
    ON mv.asset_id::TEXT = a.asset_id::TEXT 
   AND mv.reporting_date	= DATEADD('DAY',-1, (DATE_TRUNC('MONTH',CURRENT_DATE)))
  LEFT JOIN dm_risk.v_asset_value_written_off_and_lost wrl 
    ON aa.asset_id = wrl.asset_id 
   AND aa.subscription_id = wrl.subscription_id
 		ON DATE_TRUNC('MONTH', a.purchased_date)::DATE = DATE_TRUNC('MONTH', gmv.datum)::DATE
	 AND a.product_sku = gmv.product_sku
WHERE aa.subscription_id IS NOT NULL
GROUP BY 1,2
;

GRANT SELECT ON ods_production.subscription_assets TO tableau;
GRANT SELECT ON ods_production.subscription_assets TO basri_oz;