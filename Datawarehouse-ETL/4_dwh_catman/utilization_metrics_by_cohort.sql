--Base is everything that is ON LOAN performing (and not available) assets + those at risk flexibly adding those at risk
--Cohort by month of purchased_date, with results of each cohort per each month
--Long term idea: having both weekly and monthly (replacing Weekly reporting logic?)

DROP TABLE IF EXISTS dm_commercial.utilization_metrics_by_cohort;
CREATE TABLE dm_commercial.utilization_metrics_by_cohort AS
WITH purchased_base AS (
    SELECT 
    	date_trunc('month',purchased_date) AS purchased_month,
        asset_id
    FROM master.asset
    WHERE purchased_date >= dateadd('year',-3,date_trunc('year',current_date)) --from stakeholders 
)
, assets_base AS (
	 SELECT
		 date_trunc('month',ah.date) AS cohort_month,
		 ah.warehouse,
		 ah.asset_id,
		 ah.category_name,
		 ah.subcategory_name,
		 ah.product_sku,
		 ah.product_name,
		 ah.brand,
		 ah.asset_status_original,
		 ah.asset_status_new,
		 ah.dpd_bucket,     --note: an asset that had ODs might go back to performing so across different reporting months we might not see the change
		 COALESCE(ah.last_allocation_dpd,0) AS dpd_days,
		 ah.initial_price,
		 ah.purchase_price_commercial, --historically available starting september 2022
		 NULLIF(json_extract_path_text(sp.structured_specifications, 'ram'),'') AS ram,
	     pf.model_name,
	     pf.model_family
	 FROM public.dim_dates d
	  INNER JOIN master.asset_historical ah
	                     ON ah.date = d.datum
	          LEFT JOIN ods_production.product p
	                    ON p.product_sku = ah.product_sku
	          LEFT JOIN stg_api_production.spree_products sp
	                    ON sp.id = p.product_id
	          LEFT JOIN pricing.mv_product_family_check pf
	                    ON pf.product_sku = ah.product_sku
	 WHERE d.day_is_last_of_month = 1
	   AND d.datum >= dateadd('year',-3,date_trunc('year',current_date))
	 --AND asset_id = '02i0700000JTUjXAAX'
)
SELECT
    a.purchased_month,
    b.cohort_month,
    b.warehouse,
    b.category_name,
    b.subcategory_name,
    b.product_sku,
    b.product_name,
    b.brand,
    b.asset_status_new,
    b.asset_status_original,
    b.dpd_bucket,
    b.ram,
    b.model_name,
    b.model_family,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new <> 'AT RISK'
                         THEN b.initial_price
        END),0) AS initial_price_performing,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 30
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_30,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 60
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_60,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 90
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_90,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 120
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_120,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 150
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_150,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days BETWEEN 1 AND 180
                         THEN b.initial_price
        END),0) AS initial_price_at_risk_1_180,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'ON LOAN' AND b.asset_status_new = 'AT RISK' AND b.dpd_days > 1
                         THEN b.initial_price
        END
                 ),0) AS initial_price_at_risk_over_180,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'IN STOCK'
                         THEN b.initial_price
        END),0) AS initial_price_in_stock,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original = 'INBOUND UNALLOCABLE'
                         THEN b.initial_price
        END),0) AS initial_price_inbound_unallocable,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original IN (
                                                      'IN DEBT COLLECTION',
                                                      'IN REPAIR',
                                                      'IN STOCK',
                                                      'INBOUND DAMAGED',
                                                      'INBOUND QUARANTINE',
                                                      'INBOUND UNALLOCABLE',
                                                      'INCOMPLETE',
                                                      'INSOLVENCY',
                                                      'INVESTIGATION CARRIER',
                                                      'INVESTIGATION WH',
                                                      'LOCKED DEVICE',
                                                      --'ON LOAN',
                                                      'PURCHASED',
                                                      'RECOVERED',
                                                      'RESERVED',
                                                      'RETURNED',
                                                      'SENT FOR REFURBISHMENT',
                                                      'TRANSFERRED TO WH',
                                                      'WAITING FOR REFURBISHMENT',
                                                      'WARRANTY'
                         )
                         THEN b.initial_price
        END),0) AS initial_price_inventory_utilization_divisor,
    COALESCE(sum(CASE
                     WHEN b.asset_status_original IN (
                                                      'IN DEBT COLLECTION',
                                                      'IN REPAIR',
                                                      'IN STOCK',
                                                      'INBOUND DAMAGED',
                                                      'INBOUND QUARANTINE',
                                                      'INBOUND UNALLOCABLE',
                                                      'INCOMPLETE',
                                                      'INSOLVENCY',
                                                      'INVESTIGATION CARRIER',
                                                      'INVESTIGATION WH',
                                                      'IRREPARABLE',
                                                      'LOCKED DEVICE',
                                                      'LOST',
                                                      'LOST SOLVED',
                                                     -- 'ON LOAN',
                                                      'PURCHASED',
                                                      'RECOVERED',
                                                      'REPORTED AS STOLEN',
                                                      'RESERVED',
                                                      'RETURNED',
                                                      'SELLING',
                                                      'SENT FOR REFURBISHMENT',
                                                      'TRANSFERRED TO WH',
                                                      'WAITING FOR REFURBISHMENT',
                                                      'WARRANTY',
                                                      'WRITTEN OFF DC',
                                                      'WRITTEN OFF OPS')
                         THEN b.initial_price
        END),0) AS initial_price_total_utilization_divisor
    --sum(purchase_price_commercial) AS total_purchase_price_commercial
FROM purchased_base a
LEFT JOIN assets_base b 
	ON a.asset_id = b.asset_id
		AND a.purchased_month <= b.cohort_month
WHERE b.cohort_month IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;

GRANT SELECT ON dm_commercial.utilization_metrics_by_cohort TO tableau;
