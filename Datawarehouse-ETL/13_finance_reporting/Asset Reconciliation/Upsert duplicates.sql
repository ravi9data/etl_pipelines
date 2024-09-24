INSERT INTO finance.asset_reconciliation_monitoring
SELECT 
  asset_order_number
 ,variant_sku
 ,NULL AS backend_count
 ,NULL AS SAP_count
 ,NULL AS backend_amount
 ,NULL AS sap_amount
 ,'Duplicate'
 ,effective_date
 ,CURRENT_DATE AS insert_date
FROM finance.asset_reconciliation_output 
WHERE count_asset_id > 1
  AND insert_date = CURRENT_DATE
;