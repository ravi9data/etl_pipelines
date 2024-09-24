DROP TABLE IF EXISTS ods_production.asset_subscription_mapping;
CREATE TABLE ods_production.asset_subscription_mapping AS
/*SOURCE FOR LINKING ASSETS TO SUBSCRIPTIONS
 * USED IN ods_production.asset AS WELL AS MULTIPLE PAYMENT TABLES*/
SELECT
  a.subscription_id
 ,COALESCE(s.subscription_bo_id, s.subscription_id, a.subscription_id) AS subscription_id_migrated
 ,s.country_name
 ,a.customer_id
 ,a.allocation_id
 ,a.allocation_status_original
 ,a.asset_id
 ,a.return_delivery_date
 ,ast.capital_source_name AS capital_source
 ,ROW_NUMBER() OVER (PARTITION BY a.subscription_id ORDER BY a.allocated_at DESC) AS rx_last_allocation_per_sub
 ,ROW_NUMBER() OVER (PARTITION BY a.asset_id ORDER BY a.allocated_at DESC) as rx_last_allocation_per_asset
FROM ods_production.allocation a
  LEFT JOIN ods_production.subscription s 
    ON a.subscription_id = s.subscription_id
  LEFT JOIN ods_production.asset ast 
    ON a.asset_id = ast.asset_id 
WHERE a.subscription_id ILIKE 'F-%' OR s.migration_date IS NOT NULL
;