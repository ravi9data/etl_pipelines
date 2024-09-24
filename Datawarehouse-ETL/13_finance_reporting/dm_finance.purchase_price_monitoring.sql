CREATE OR REPLACE VIEW dm_finance.purchase_price_monitoring AS 
WITH avg_pur_price AS (
SELECT
  variant_sku ,
  ROUND(AVG(initial_price),2) avg_purchase_price,
  COUNT(DISTINCT asset_id) AS no_of_assets
FROM master.asset a
GROUP BY 1
)
SELECT
  a.variant_sku ,
  a.asset_id ,
  a.serial_number ,
  a.product_name,
  a.product_sku,
  a.supplier,
  a.first_allocation_store,
  a.invoice_number,
  a.request_id,
  a.purchase_request_item_id,
  req.purchase_order_number,
  a.capital_source_name ,
  a.purchased_date ,
  b.avg_purchase_price,
  a.initial_price ,
  b.no_of_assets,
  ROUND(a.initial_price / NULLIF(b.avg_purchase_price,0),2) AS ratio
FROM master.asset a
  INNER JOIN avg_pur_price b ON
    a.variant_sku = b.variant_sku
  LEFT JOIN ods_production.purchase_request_item req
    ON a.purchase_request_item_sfid = req.purchase_request_item_sfid
WHERE TRUE   
  AND (ratio < 0.5 OR ratio > 1.5)
  AND a.capital_source_name IN ('Grover Finance I GmbH','SUSTAINABLE TECH RENTAL EUROPE II GMBH','USA_test')
WITH NO SCHEMA BINDING
;
