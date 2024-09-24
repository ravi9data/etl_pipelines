INSERT INTO finance.asset_reconciliation_monitoring
SELECT
  asset_order_number
 ,variant_sku
 ,NULL AS backend_count
 ,NULL AS SAP_count
 ,backend_amount
 ,sap_amount
 ,'Amount' AS description 
 ,effective_date
 ,insert_date 
FROM finance.asset_reconciliation_amount_monitoring
WHERE backend_amount - sap_amount > 2
;