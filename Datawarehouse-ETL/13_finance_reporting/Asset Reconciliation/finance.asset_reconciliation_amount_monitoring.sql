TRUNCATE TABLE finance.asset_reconciliation_amount_monitoring;

INSERT INTO finance.asset_reconciliation_amount_monitoring
WITH amount_asset AS (
SELECT 
  asset_order_number
 ,variant_sku
 ,effective_date
 ,COUNT(asset_order_number) * MAX(initial_price) AS backend_amount
FROM finance.asset_reconciliation_output 
WHERE insert_date = CURRENT_DATE
GROUP BY 1,2,3
)
,amount_sap AS (
SELECT
  t.tax_rate AS sap_tax
 ,s."item no." 
 ,s.purchaseid
 ,DATE_TRUNC('MONTH', TO_TIMESTAMP("posting date",'DD/MM/YYYY')::TIMESTAMP) AS posting_month
 ,SUM(quantity::BIGINT) * ((MAX(price::DOUBLE PRECISION) * sap_tax) + MAX(price::DOUBLE PRECISION)) AS sap_amount
FROM finance.asset_reconciliation_sap_invoice_import   s  
  LEFT JOIN finance.asset_reconciliation_sap_tax_definition AS t
    ON s."tax definition" = t.sap_tax_group 
GROUP BY 1,2,3,4
)
SELECT  
  COALESCE(a.asset_order_number,b.purchaseid) AS asset_order_number
 ,COALESCE(a.variant_sku,"item no.") AS variant_sku
 ,COALESCE(a.backend_amount,0) AS backend_amount
 ,COALESCE(b.sap_amount,0) AS sap_amount
 ,'Amount' AS description
 ,effective_date
 ,CURRENT_DATE AS insert_date
FROM amount_sap b
  LEFT JOIN amount_asset a
    ON a.asset_order_number = b.purchaseid
   AND variant_sku = b."item no."
   AND posting_month = effective_date
;