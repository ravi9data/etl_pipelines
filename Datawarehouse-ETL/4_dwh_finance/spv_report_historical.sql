DROP TABLE IF EXISTS dm_finance.spv_report_historical;
CREATE TABLE dm_finance.spv_report_historical AS
WITH active_value AS (
SELECT DISTINCT
 LAST_DAY(s."date") AS reporting_month,
 s.subscription_id,
 a.asset_id,
 MIN(s.subscription_value) AS subscription_value,
 MAX(s.subscription_value) AS subscription_value_max
FROM master.subscription_historical s
  LEFT JOIN master.allocation_historical a
    ON s.subscription_id = a.subscription_id
    AND s."date" = a."date"
WHERE TRUE 
   AND a.allocation_id IS NOT NULL
   AND (s.status = 'ACTIVE' 
        OR COALESCE(s.cancellation_date::DATE, a.refurbishment_end_at) = DATE_TRUNC('month', s.date)::DATE)
GROUP BY 1, 2, 3
)
,ranks AS (
SELECT
 reporting_month,
 subscription_id,
 asset_id,
 subscription_value,
 CASE
   WHEN subscription_value_max > subscription_value
    THEN 1
   ELSE 0
 END AS sub_value_change, 
 subscription_value_max - subscription_value AS sub_value_change_value,
 RANK() OVER (PARTITION BY asset_id, subscription_id ORDER BY reporting_month) AS rank_,
 CASE
  WHEN rank_ = 1
   THEN 1
  ELSE 0
 END AS is_acquired 
FROM active_value
)
,active_value_final AS (
SELECT
 asset_id,
 reporting_month,
 SUM(subscription_value) AS active_subscription_value,
 SUM(CASE
  WHEN sub_value_change = 1
   THEN sub_value_change_value
 END) AS sub_value_change, 
 SUM(CASE
  WHEN is_acquired = 1
   THEN subscription_value
 END) AS acquired_subscription_value, 
 SUM(CASE
  WHEN is_acquired = 0
   THEN subscription_value
 END) AS rollover_subscription_value
FROM ranks
GROUP BY 1, 2
)
,cancelled_value AS (
SELECT DISTINCT
 LAST_DAY(s."date") AS reporting_month,
 s.subscription_id,
 a.asset_id,
 MIN(s.subscription_value) AS subscription_value,
 MAX(CASE
  WHEN COALESCE(s.cancellation_date::DATE, a.refurbishment_end_at) + 1 = s.date::DATE
   THEN 1
  ELSE 0
 END) AS IS_CANCELLED
FROM master.subscription_historical s
  LEFT JOIN master.allocation_historical a
    ON s.subscription_id = a.subscription_id
   AND s."date" = a."date"
WHERE TRUE     
  AND COALESCE(s.cancellation_date::DATE, a.refurbishment_end_at) + 1 = s.date::DATE
  AND COALESCE(s.cancellation_date::DATE, a.refurbishment_end_at) <> a.allocated_at::DATE
GROUP BY 1, 2, 3
)
,rank_ AS (
SELECT
 reporting_month,
 subscription_id,
 asset_id,
 subscription_value,
 RANK() OVER (PARTITION BY asset_id, subscription_id ORDER BY reporting_month) AS rank_,
 COUNT(*) OVER (PARTITION BY asset_id, subscription_id) AS count_ttal,
 CASE
  WHEN RANK() OVER (PARTITION BY asset_id, subscription_id 
   ORDER BY reporting_month) = COUNT(*) OVER (PARTITION BY asset_id, subscription_id)
   THEN 1
  ELSE 0
 END AS is_cancelled
FROM cancelled_value
)
,cancelled_value_final AS (
SELECT
 asset_id,
 reporting_month,
 SUM(CASE
  WHEN is_cancelled = 1
   THEN subscription_value
 END) AS cancelled_subscription_value
FROM rank_
GROUP BY 1, 2
)
, new_to_portfolio AS (
SELECT
   c.*
FROM dwh.asset_capitalsource_change c
  INNER JOIN
      (SELECT DISTINCT
        c.asset_id,
        MAX(c.created_at ) OVER (PARTITION BY c.asset_id , DATE_TRUNC('month', created_at)) AS created_at
       FROM dwh.asset_capitalsource_change c
      ) b
    ON c.asset_id = b.asset_id
   AND c.created_at = b.created_at
)
SELECT DISTINCT
 m.reporting_date,
 m.capital_source_name,
 LAG(m.capital_source_name) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_capital_source_name,
 m.asset_id,
 m.serial_number,
 m.product_sku,
 a.brand,
 m.category,
 m.subcategory,
 m.asset_name,
 m."condition" AS asset_condition_spv,
  CASE
      WHEN
         DATE_TRUNC('MONTH', a.created_at::DATE) = DATE_TRUNC('MONTH', m.reporting_date::DATE)
      THEN
         1
      ELSE
         0
   END
 AS asset_added_to_portfolio,
   CASE
      WHEN
         DATE_TRUNC('MONTH', atr.date_transfer_assets::DATE) = DATE_TRUNC('MONTH', m.reporting_date::DATE)
      THEN
         1
      ELSE
         0
   END
   AS asset_added_to_portfolio_m_and_g,
    CASE
         WHEN
            m.asset_status_original IN
            (
               'WRITTEN OFF OPS', 'WRITTEN OFF DC', 'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            'REMOVED'
         WHEN
            asset_added_to_portfolio = 1
         THEN
            'NEW'
         ELSE
            'EXISTING'
      END
      AS asset_classification,
      CASE
         WHEN
            m.asset_status_original IN
            (
               'WRITTEN OFF OPS', 'WRITTEN OFF DC', 'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            'REMOVED'
         WHEN
            asset_added_to_portfolio_m_and_g = 1
         THEN
            'NEW'
         ELSE
            'EXISTING'
      END
      AS asset_classification_m_and_g, 
 CASE
  WHEN h.new_value IS NOT NULL
   THEN 1
  WHEN DATE_TRUNC('month', a.created_at)::DATE = DATE_TRUNC('month', m.reporting_date)::DATE
   THEN 1
  ELSE 0
 END AS new_to_portfolio, 
 LAG(m.condition) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_asset_condition_spv, 
 m.purchased_date, a.created_at, 
 m.valuation_method, 
 LAG(m.valuation_method) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_valuation_method, 
 m.initial_price, 
 a.market_price_at_purchase_date,
 a.purchase_price_commercial,
 m.days_in_stock AS last_allocation_days_in_stock, 
 m_since_last_valuation_price, 
 LAG(m.m_since_last_valuation_price) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_m_since_last_valuation_price, 
 m.valuation_1, 
 m.valuation_2, 
 m.valuation_3,
 CASE
  WHEN a.asset_status_original like 'WRITTEN OFF%'
   THEN 0
  ELSE final_price
 END AS final_price, 
 LAG(final_price) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_final_price, 
 m.asset_status_original, 
 LAG(m.asset_status_original) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_asset_status_original,
 CASE
  WHEN LAG(m.asset_id) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) IS NULL
   THEN '0'
  ELSE '1'
 END AS ppp,
 CASE
  WHEN LAG(m.condition) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) = 'AGAN'
    AND condition = 'AGAN'
   THEN 1
  ELSE 0
 END AS is_agan, 
 a.asset_status_new, 
 a.asset_status_detailed, 
 a.last_allocation_dpd, 
 COALESCE(CASE
  WHEN a.asset_status_original IN ('IRREPARABLE', 'RECOVERED')
   THEN 'IN STOCK'
    ELSE a.dpd_bucket
 END,
 CASE
  WHEN a.last_allocation_dpd = 0
   THEN 'PERFORMING'
  WHEN a.last_allocation_dpd > 0 AND a.last_allocation_dpd < 31
   THEN 'OD 1-30'
  WHEN a.last_allocation_dpd > 30 AND a.last_allocation_dpd < 61
   THEN'OD 31-60'
  WHEN a.last_allocation_dpd > 60 AND a.last_allocation_dpd < 91
   THEN 'OD 61-90'
  WHEN a.last_allocation_dpd > 90 AND a.last_allocation_dpd < 121
   THEN 'OD 91-120'
  WHEN a.last_allocation_dpd > 120 AND a.last_allocation_dpd < 151
   THEN 'OD 121-150'
  WHEN a.last_allocation_dpd > 150 AND a.last_allocation_dpd < 180
   THEN 'OD 151-180'
  WHEN a.last_allocation_dpd > 180
   THEN 'OD 180+'
  WHEN a.asset_status_original = 'SOLD'
   THEN 'SOLD'
  WHEN a.asset_status_original = 'LOST' OR a.asset_status_original = 'LOST SOLVED'
   THEN 'LOST'
  WHEN a.active_subscriptions > 0
   THEN'PERFORMING'
  ELSE 'IN STOCK'
 END) AS dpd_bucket, 
 LAG(a.asset_status_new) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_asset_status_new, 
 LAG(a.asset_status_detailed) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_asset_status_detailed, 
 LAG(a.last_allocation_dpd) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE) AS prev_last_allocation_dpd, 
 LAG( COALESCE(a.dpd_bucket, CASE
  WHEN a.last_allocation_dpd = 0
   THEN 'PERFORMING'
  WHEN a.last_allocation_dpd > 0 AND a.last_allocation_dpd < 31
   THEN 'OD 1-30'
  WHEN a.last_allocation_dpd > 30 AND a.last_allocation_dpd < 61
   THEN 'OD 31-60'
  WHEN a.last_allocation_dpd > 60 AND a.last_allocation_dpd < 91
   THEN 'OD 61-90'
  WHEN a.last_allocation_dpd > 90 AND a.last_allocation_dpd < 121
   THEN 'OD 91-120'
  WHEN a.last_allocation_dpd > 120 AND a.last_allocation_dpd < 151
   THEN 'OD 121-150'
  WHEN a.last_allocation_dpd > 150 AND a.last_allocation_dpd < 180
   THEN 'OD 151-180'
  WHEN a.last_allocation_dpd > 180
   THEN 'OD 180+'
  WHEN a.asset_status_original = 'SOLD'
   THEN 'SOLD'
  WHEN a.asset_status_original = 'LOST' OR a.asset_status_original = 'LOST SOLVED'
   THEN 'LOST'
  WHEN a.active_subscriptions > 0
   THEN 'PERFORMING'
  ELSE 'IN STOCK'
 END
 )) OVER (PARTITION BY m.asset_id ORDER BY m.reporting_date::DATE - 1) AS prev_dpd_bucket, 
 GREATEST(COALESCE(avf.sub_value_change, 0), 0) AS sub_value_change, 
 GREATEST(COALESCE(avf.rollover_subscription_value, 0), 0) AS rollover_subscription_value, 
 GREATEST(COALESCE(cvf.cancelled_subscription_value, 0)) AS cancelled_subscription_value, 
 GREATEST(COALESCE(avf.acquired_subscription_value, 0), 0) AS acquired_subscription_value, 
 GREATEST( (COALESCE(avf.rollover_subscription_value, 0) + COALESCE(avf.acquired_subscription_value , 0) 
          - COALESCE(cvf.cancelled_subscription_value, 0)) , 0) AS active_subscription_value, 
 s.store_commercial, 
 s.rental_period, 
 m.warehouse,
 a.variant_sku,
 a.ean
 FROM ods_production.spv_report_master m
  LEFT JOIN master.asset_historical a
    ON a.date::DATE = m.reporting_date::DATE
   AND a.asset_id = m.asset_id
  LEFT JOIN master.subscription s
    ON s.subscription_id = a.subscription_id
  LEFT JOIN active_value_final avf
    ON avf.asset_id = m.asset_id
   AND avf.reporting_month::DATE = m.reporting_date::DATE
  LEFT JOIN cancelled_value_final cvf
    ON cvf.asset_id = m.asset_id
   AND cvf.reporting_month::DATE = m.reporting_date::DATE
  LEFT JOIN finance.asset_transferred_to_m_and_g atr
   ON m.asset_id = atr.asset_id
  LEFT JOIN new_to_portfolio h
    ON h.asset_id = m.asset_id
   AND DATE_TRUNC('month', m.reporting_date) = DATE_TRUNC('month', h.created_at)
WHERE m.reporting_date::DATE = LAST_DAY(m.reporting_date)
   OR m.reporting_date::DATE = CURRENT_DATE - 1;


GRANT SELECT ON dm_finance.spv_report_historical TO redash_pricing;
GRANT SELECT ON dm_finance.spv_report_historical TO hightouch_pricing;
GRANT SELECT ON dm_finance.spv_report_historical TO tableau;
GRANT SELECT ON dm_finance.spv_report_historical TO GROUP mckinsey;
