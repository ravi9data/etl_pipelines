DELETE FROM finance.asset_reconciliation_output 
WHERE insert_date = CURRENT_DATE;	

INSERT INTO finance.asset_reconciliation_output						   
WITH m_pre AS (
SELECT DISTINCT 
  s.purchase_id AS sap_purchase_id
 ,s.item_no AS sku
 ,s.invoice_date::DATE AS invoice_date
 ,s.price::DECIMAL(10,2) AS sap_price
 ,s.tax_definition
 ,t.tax_rate AS sap_tax
 ,ROW_NUMBER() OVER (PARTITION BY sap_purchase_id, sku ORDER BY s.invoice_date DESC) AS idx
 ,COUNT(*) OVER (PARTITION BY sap_purchase_id, sku) AS cnt
 ,s.insert_date
 ,s.effective_date
FROM finance.asset_reconciliation_sap_invoice_import_historical AS s
  LEFT JOIN finance.asset_reconciliation_sap_tax_definition AS t
    ON s.tax_definition = t.sap_tax_group 
WHERE TRUE 
  AND s.purchase_id IS NOT NULL
  AND insert_date = CURRENT_DATE
)
,m AS (
SELECT DISTINCT 
  sap_purchase_id
 ,sku
 ,tax_definition
 ,sap_tax
 ,invoice_date
 ,sap_price
 ,insert_date
 ,effective_date
FROM m_pre 	 
WHERE idx = 1
)	
SELECT DISTINCT 	
  COUNT(ah.asset_id) OVER (PARTITION BY ah.asset_id) count_asset_id
 ,COALESCE(req.purchase_order_number, ast.asset_order_number) AS asset_order_number
 ,ah.asset_id
 ,ah.asset_name
 ,ah.variant_sku
 ,ah.serial_number
 ,ah.category_name
 ,ah.subcategory_name
 ,ast.brand
 ,ah.purchased_date
 ,m.sap_price
 ,ah.initial_price
 ,m.sap_purchase_id
 ,m.tax_definition
 ,m.sap_tax
 ,m.invoice_date
 ,m.insert_date
 ,m.effective_date
 ,COALESCE(acm.asset_class, acm2.asset_class) AS asset_class
FROM m 
  LEFT JOIN ods_production.purchase_request_item req
    ON m.sap_purchase_id = req.purchase_order_number 
   AND m.sku = req.variant_sku
  LEFT JOIN ods_production.asset ast 
    ON ast.purchase_request_item_id = req.purchase_request_item_sfid
  LEFT JOIN master.asset_historical ah 
    ON ah.asset_id = ast.asset_id 
  LEFT JOIN finance.asset_reconciliation_asset_classes_mapping_2022_prior acm
    ON ah.category_name = acm.category_name
 	 AND ah.subcategory_name = acm.subcategory_name
 	 AND ah.brand = acm.brand
  LEFT JOIN finance.asset_reconciliation_asset_classes_mapping_2022_prior acm2
    ON acm2.category_name = ah.category_name
 	 AND acm2.subcategory_name = ah.subcategory_name
   AND acm2.brand IS NULL	 
WHERE TRUE 
  AND ah.asset_id IS NOT NULL 
  AND ah."date" = insert_date - 1 
  AND ast.asset_status_original <> 'RETURNED TO SUPPLIER'
ORDER BY 1 DESC;