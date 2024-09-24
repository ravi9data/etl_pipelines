DELETE FROM finance.asset_reconciliation_monitoring
WHERE insert_date = CURRENT_DATE;

INSERT INTO finance.asset_reconciliation_monitoring
SELECT  
  asset_order_number
 ,variant_sku
 ,backend_count
 ,SAP_count
 ,NULL AS backend_amount
 ,NULL AS sap_amount
 ,'Count' AS description
 ,effective_date
 ,insert_date
FROM finance.asset_reconciliation_quantity_monitoring
WHERE COALESCE(backend_count, 0) <> COALESCE(SAP_count, 0)
;