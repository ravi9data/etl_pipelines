DELETE FROM dm_finance.new_way_of_buying;

INSERT INTO dm_finance.new_way_of_buying
WITH pricing_costs_base AS (
SELECT DISTINCT subcategory_name
FROM ods_production.product
)
,pricing_costs AS (
-- REPAIR (note some alterations from Esteban on values)
SELECT 
  a.subcategory_name,
  COALESCE(b.median_repair_pppct::float, 25.0) AS median_repair_pppct,
  COALESCE(b.repair_rate_pct::float, 25.0) AS repair_rate_pct,
  COALESCE(b.avg_refurb_cost::float, 50.0) AS avg_refurb_cost
FROM pricing_costs_base a
  LEFT JOIN staging_airbyte_bi.pricing_costs b
    ON a.subcategory_name = b.subcategory_name
)
,asset_data AS (
SELECT 
  product_sku,
  subcategory_name,
  AVG(purchase_price_commercial) AS avg_ppc,
  SUM(purchase_price_commercial) AS sum_ppc,
  SUM(months_since_purchase) AS sum_months_old,
  SUM(subscription_revenue) AS sum_sub_rev,
  DATEDIFF(YEAR, MAX(purchased_date)::DATE, CURRENT_DATE) AS most_recent_purchase_year,
  COUNT(DISTINCT asset_id) AS total_assets,
  COUNT(DISTINCT CASE
   WHEN DATEDIFF(MONTH, purchased_date::DATE, CURRENT_DATE) < 6 
    THEN asset_id
  END) AS assets_last_6_months,
  COALESCE(100.0 * assets_last_6_months::FLOAT / NULLIF(total_assets::FLOAT, 0), 0) AS purchasing_rate,
  SUM(CASE
   WHEN asset_status_new IN ('PERFORMING', 'AT RISK') 
    THEN purchase_price_commercial
  END) AS inv_util_numerator,
  SUM(CASE
   WHEN asset_status_new IN ('PERFORMING', 'IN REPAIR', 'REFURBISHMENT', 'IN STOCK', 'AT RISK') 
    THEN purchase_price_commercial
  END) AS inv_util_denom,
  COALESCE(100.0 * inv_util_numerator::FLOAT / NULLIF(inv_util_denom::FLOAT, 0), 0) AS inv_util
FROM master.asset
GROUP BY 1, 2
)
,repair_estimate AS (
SELECT 
  product_sku,
  AVG(price_brutto) AS avg_rep_cost
FROM dm_recommerce.repair_cost_estimates
GROUP BY 1
)
,return_cost AS (
SELECT 
  product_sku AS product_sku_1,
  LEAST(NULLIF(return_ups_de_de_final, '#N/A')::FLOAT, NULLIF(return_ups_nl_de_final, '#N/A')::FLOAT, 
        NULLIF(return_hermes_de_final, '#N/A')::FLOAT, NULLIF(return_dhl_de_final, '#N/A')::FLOAT)::FLOAT AS de_return_cost,
  LEAST(NULLIF(return_ups_de_at_final, '#N/A')::FLOAT, NULLIF(return_ups_nl_at_final, '#N/A')::FLOAT,
        NULLIF(return_hermes_at_final, '#N/A')::FLOAT, NULLIF(return_dhl_at_final, '#N/A')::FLOAT)::FLOAT AS at_return_cost,
  LEAST(NULLIF(return_ups_de_nl_final, '#N/A')::FLOAT, NULLIF(return_ups_nl_nl_final, '#N/A')::FLOAT,
        NULLIF(return_hermes_nl_final, '#N/A')::FLOAT, NULLIF(return_dhl_nl_final, '#N/A')::FLOAT)::FLOAT AS nl_return_cost,
  LEAST(NULLIF(return_ups_de_es_final, '#N/A')::FLOAT, NULLIF(return_ups_nl_es_final, '#N/A')::FLOAT,
        NULLIF(return_hermes_es_final, '#N/A')::FLOAT, NULLIF(return_dhl_es_final, '#N/A')::FLOAT)::FLOAT AS es_return_cost
FROM staging_airbyte_bi.shipping_costs_k8s
)
,sku_data AS (
SELECT 
  a.product_sku, 
-- % of new assets bought in last 6 months, scored
  CASE
   WHEN a.purchasing_rate > 50
    THEN 1
   WHEN a.purchasing_rate > 10
    THEN 2
   ELSE 3
  END AS investment_score, 
-- % inventory utilisation, scored
  CASE
   WHEN a.inv_util > 80 
    THEN 1
   WHEN a.inv_util > 60 
    THEN 2
   ELSE 3
  END AS inv_util_score, 
-- Estimate of repair cost per SKU as % of avg purchase price, scored
  CASE
   WHEN 100.0 * d.avg_rep_cost / NULLIF(a.avg_ppc, 0) > 100
    THEN 3
   WHEN 100.0 * d.avg_rep_cost / NULLIF(a.avg_ppc, 0) < 25 
    THEN 1
   ELSE 2
  END AS repair_score, 
-- Most recent investment, scored
  CASE
   WHEN a.most_recent_purchase_year <= 1
    THEN 1
   WHEN a.most_recent_purchase_year <= 2 
    THEN 2
   WHEN a.most_recent_purchase_year <= 3 
    THEN 3
   ELSE 4
  END AS investment_purchase_score,
  a.inv_util,
  100.0 * COALESCE((a.sum_sub_rev / NULLIF(a.sum_months_old, 0)) / NULLIF(a.sum_ppc, 0), 0) AS avg_monthly_ir,
  t.avg_refurb_cost,
  t.median_repair_pppct / 100.0 * t.repair_rate_pct / 100.0 AS avg_repair_cost_pppct,
  a.avg_ppc,
  c.*
FROM asset_data a
  LEFT JOIN repair_estimate d
    ON a.product_sku = d.product_sku
  LEFT JOIN pricing_costs t 
    ON a.subcategory_name = t.subcategory_name
  LEFT JOIN return_cost c 
    ON a.product_sku = c.product_sku_1
WHERE a.product_sku IS NOT NULL 
)
,subscription_data AS (
SELECT 
  subscription_id,
  product_sku,
  product_name,
  category_name,
  subcategory_name,
  brand,
  order_created_date,
  COALESCE(subscription_bo_id, subscription_sf_id, subscription_id) AS contract_id,
  months_required_to_own,
  ROUND(MONTHS_BETWEEN(CURRENT_DATE, order_created_date::DATE), 2) AS months_rented,
  minimum_term_months, 
-- cash collected
  subscription_revenue_paid, 
-- cash expected
  subscription_revenue_due,
  subscription_value,
  country_name,
  customer_type,
  CASE
   WHEN DATEDIFF(DAY, minimum_cancellation_date::DATE, CURRENT_DATE) >= -7 
    THEN TRUE
   ELSE FALSE 
  END AS eligible_for_new_po
FROM master.subscription
WHERE TRUE 
  AND cancellation_date IS NULL
  AND subscription_id IS NOT NULL
  AND product_sku IS NOT NULL
  AND ((order_created_date >= '2023-07-25 12:00:00' AND store_id IN (1, 4, 618, 126, 627, 622, 626, 629)) -- DE, AT, ES, B2B DE,ES,AT,NL,US
   OR 
       (order_created_date::DATE >= '2023-07-31' AND store_id NOT IN (5, 621)) -- Just not NL, US, to include partners here
      ) 
)
,allocation_pre AS (
SELECT
  ah.asset_id,
  ah.allocation_id,
  a.order_id,
  ah.allocation_status_original,
  COALESCE(sh.subscription_id, ah.subscription_id) AS subscription_id,
  sh.start_date,
  sh.status,
  ah2.sold_date,
  ah2.sold_price,
  ROW_NUMBER() OVER (PARTITION BY ah.asset_id ORDER BY ah.allocated_at DESC) idx
FROM master.allocation ah
  LEFT JOIN ods_production.allocation a
    ON a.allocation_id = ah.allocation_id
  LEFT JOIN master.subscription sh
    ON ah.subscription_id = sh.subscription_id
  LEFT JOIN master.asset ah2
    ON ah2.asset_id = ah.asset_id
)
,allocation AS (
SELECT
  *
FROM allocation_pre
WHERE
  idx = 1
)
,luxco_reporting_date AS (
SELECT
  MAX(reporting_date) AS max_luxco_reporting_date
FROM dm_finance.spv_report_historical
)
-- ASSET level
,asset_data_luxco_1 AS (
SELECT 
  a.subscription_id,
  a.asset_id,
  a.purchase_price_commercial,
  a.asset_status_new,
  spv.final_price,
  ROW_NUMBER() OVER (PARTITION BY a.subscription_id ORDER BY spv.created_at DESC) AS rn
FROM master.asset a
  LEFT JOIN dm_finance.spv_report_historical spv
    ON a.asset_id = spv.asset_id
  INNER JOIN luxco_reporting_date l
    ON spv.reporting_date  = l.max_luxco_reporting_date  
  LEFT JOIN allocation AS ah
    ON a.asset_id = ah.asset_id
  INNER JOIN subscription_data AS s
    ON ah.subscription_id = s.subscription_id  
)
,asset_data_luxco AS (
SELECT *
FROM asset_data_luxco_1
WHERE rn = 1 -- Dedup for subscriptions with more than 1 asset_id
)
,collated_data AS(
SELECT 
  s.*,
  COALESCE(a.asset_status_new, 'UNKNOWN') AS asset_status_new,
  COALESCE(a.purchase_price_commercial, k.avg_ppc, 0) AS pp_commercial,
  COALESCE(a.final_price, a.purchase_price_commercial, k.avg_ppc, 0) AS asset_value,
  k.investment_score,
  k.inv_util_score,
  k.repair_score,
  k.investment_purchase_score,
  k.inv_util,
  k.avg_monthly_ir,
  k.avg_refurb_cost,
  k.avg_repair_cost_pppct,
  k.avg_repair_cost_pppct * pp_commercial AS avg_repair_cost,
  CASE
   WHEN s.country_name = 'Germany' 
    THEN COALESCE(k.de_return_cost, 20)
   WHEN s.country_name = 'Austria'
    THEN COALESCE(k.at_return_cost, 20)
   WHEN s.country_name = 'Netherlands'
    THEN COALESCE(k.nl_return_cost, 20)
   WHEN s.country_name = 'Spain'
    THEN COALESCE(k.es_return_cost, 20)
   ELSE COALESCE(k.de_return_cost, 20)
  END AS return_cost,
  CASE
   WHEN s.months_required_to_own <= 0
    THEN 1.3 * pp_commercial
   WHEN s.months_required_to_own IS NULL
    THEN 1.3 * pp_commercial
   ELSE s.months_required_to_own::FLOAT * s.subscription_value
  END AS old_po_target,
  k.inv_util_score + k.repair_score + k.investment_purchase_score AS total_score,
  CASE
   WHEN k.inv_util >= 90 
    THEN LEAST(5, total_score)
   ELSE total_score
  END AS total_score_adjusted,
  CASE
   WHEN total_score_adjusted <= 3
    THEN 1.0
   WHEN total_score_adjusted = 4
    THEN 0.9
   WHEN total_score_adjusted = 5
    THEN 0.85
   WHEN total_score_adjusted = 6
    THEN 0.8
   WHEN total_score_adjusted = 7
    THEN 0.7
   WHEN total_score_adjusted >= 8
    THEN 0.5
  END AS multiplier_1,
  0.01 * POW(total_score_adjusted -3, 2) AS multiplier_2,
  CASE
   WHEN total_score_adjusted = 3
    THEN 0
   WHEN total_score_adjusted = 4
    THEN 0.5
   ELSE 1.0
  END AS multiplier_3,
  asset_value * multiplier_1 AS final_asset_value,
  (k.avg_refurb_cost + return_cost +(avg_repair_cost * multiplier_3)) * multiplier_2 AS final_cost_of_return,
  s.subscription_revenue_paid * 0.05 AS final_reward_value,
  final_asset_value - final_cost_of_return - final_reward_value AS new_po_value,
  GREATEST(old_po_target - s.subscription_revenue_paid, 1) AS old_po_value,
  GREATEST(old_po_value, new_po_value) AS final_po_value,
  CASE 
   WHEN GREATEST(old_po_value, new_po_value) = old_po_value 
    THEN 'old_po_value'
   WHEN GREATEST(old_po_value, new_po_value) = new_po_value 
    THEN 'new_po_value'
   ELSE NULL 
  END AS po_value_source
FROM subscription_data s
  LEFT JOIN asset_data_luxco a
    ON a.subscription_id = s.subscription_id
  LEFT JOIN sku_data k
    ON k.product_sku = s.product_sku
)
SELECT 
  contract_id,
  subscription_id,
  final_po_value,
  po_value_source,
  CASE
   WHEN eligible_for_new_po IS TRUE 
     AND asset_status_new = 'PERFORMING' 
    THEN TRUE
   ELSE FALSE
  END AS eligible,
  CASE
    WHEN eligible_for_new_po IS FALSE 
      AND asset_status_new <> 'PERFORMING' 
     THEN 'Subscription has not reached minimum duration. Asset is marked as not performing; Check payments.'
    WHEN eligible_for_new_po IS FALSE 
      AND asset_status_new = 'PERFORMING' 
     THEN 'Subscription has not reached minimum duration.'
    WHEN eligible_for_new_po IS TRUE 
      AND asset_status_new <> 'PERFORMING' 
     THEN 'Asset is marked as not performing; Check payments.'
    WHEN eligible_for_new_po IS TRUE 
      AND asset_status_new = 'PERFORMING' 
     THEN 'Eligible for purchase quote.'
  END AS reason_eligible
FROM collated_data
;
