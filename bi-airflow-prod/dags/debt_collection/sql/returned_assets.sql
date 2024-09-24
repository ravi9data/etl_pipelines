DROP TABLE IF EXISTS  dm_debt_collection.returned_assets;

CREATE TABLE dm_debt_collection.returned_assets AS
WITH payment_subs AS (
SELECT DISTINCT
  subscription_id,
  asset_return_date
FROM ods_production.payment_subscription
)
, allocations AS (
SELECT
  al.subscription_id,
  al.asset_id,
  al.allocation_status_original,
  a.residual_value_market_price AS finco_market_valuation,
  s.outstanding_residual_asset_value,
  al.is_last_allocation_per_asset,
  al.return_shipment_label_created_at,
  al.return_shipment_at,
  al.return_delivery_date
FROM master.allocation al
  LEFT JOIN master.subscription s 
    ON al.subscription_id = s.subscription_id 
  LEFT JOIN master.asset a
    ON al.asset_id = a.asset_id
QUALIFY ROW_NUMBER () OVER (PARTITION BY al.subscription_id ORDER BY al.allocated_at DESC) = 1 
)
SELECT
  pr.contact_date::DATE,
  pr.contact_type,
  pr.subscription_id,
  pr.country,
  pr.agent,
  pr.dca,
  ps.asset_return_date,
  a.return_shipment_label_created_at,
  a.return_shipment_at,
  a.return_delivery_date,
  a.asset_id,
  a.allocation_status_original,
  a.finco_market_valuation,
  a.outstanding_residual_asset_value
FROM staging_airbyte_bi.return_assets pr
  INNER JOIN master.subscription s
    ON pr.subscription_id = COALESCE (s.subscription_sf_id, s.subscription_id)
  LEFT JOIN payment_subs ps
    ON s.subscription_id = ps.subscription_id
  LEFT JOIN allocations a
    ON s.subscription_id = a.subscription_id
;

GRANT SELECT ON dm_debt_collection.returned_assets TO tableau;
