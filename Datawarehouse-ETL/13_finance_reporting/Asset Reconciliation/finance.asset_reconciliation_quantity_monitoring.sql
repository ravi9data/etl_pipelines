TRUNCATE TABLE finance.asset_reconciliation_quantity_monitoring;

INSERT INTO finance.asset_reconciliation_quantity_monitoring
WITH quantity_asset AS (
SELECT 
  asset_order_number
 ,variant_sku
 ,effective_date
 ,COUNT(*) AS backend_count
FROM finance.asset_reconciliation_output
WHERE insert_date = CURRENT_DATE
GROUP BY 1,2,3
)
,quantity_sap AS (
SELECT 
  purchaseid AS purchaseid
 ,"item no."
 ,DATE_TRUNC('MONTH', TO_TIMESTAMP("posting date",'DD/MM/YYYY')::TIMESTAMP) AS posting_month
 ,SUM(quantity::BIGINT) AS SAP_count
FROM finance.asset_reconciliation_sap_invoice_import  
GROUP BY 1,2,3
)
SELECT 
  COALESCE(a.asset_order_number, b.purchaseid) AS asset_order_number
 ,COALESCE(variant_sku, "item no.") AS variant_sku 
 ,a.backend_count
 ,b.SAP_count
 ,'Count' AS description
 ,effective_date
 ,CURRENT_DATE AS insert_date
FROM quantity_sap b
  LEFT JOIN quantity_asset a
    ON a.asset_order_number = b.purchaseid 
   AND variant_sku = b."item no."
   AND effective_date = posting_month
 ;