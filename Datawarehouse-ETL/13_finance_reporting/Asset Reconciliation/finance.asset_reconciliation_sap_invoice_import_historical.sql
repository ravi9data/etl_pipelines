DELETE FROM finance.asset_reconciliation_sap_invoice_import_historical 
WHERE insert_date = CURRENT_DATE; 

INSERT INTO finance.asset_reconciliation_sap_invoice_import_historical
( customer_supplier_ref_no
 ,purchase_id
 ,document_number
 ,customer_supplier_no
 ,customer_supplier_name
 ,posting_date
 ,invoice_date
 ,item_no
 ,item_service_description
 ,quantity
 ,price
 ,discount_per_row
 ,tax_definition
 ,gross_price_after_discount
 ,row_total
 ,distribution_rule
 ,uom_code
 ,insert_date
 ,effective_date
)
SELECT 
  "customer/supplier ref. no."
 ,"purchaseid"
 ,"document number"::BIGINT
 ,"customer/supplier no."::BIGINT
 ,"customer/supplier name"
 ,TO_TIMESTAMP("posting date",'DD/MM/YYYY')::TIMESTAMP 
 ,TO_TIMESTAMP("document date",'DD/MM/YYYY')::TIMESTAMP
 ,"item no."
 ,"item/service description"
 ,"quantity"::BIGINT
 ,"price"::DOUBLE PRECISION
 ,"discount % per row"::DOUBLE PRECISION
 ,"tax definition"
 ,"gross price after discount"::DOUBLE PRECISION
 ,"row total"::DOUBLE PRECISION
 ,"distribution rule"
 ,"uom code"
 ,CURRENT_DATE
 ,DATE_TRUNC('MONTH', TO_TIMESTAMP("posting date",'DD/MM/YYYY')::TIMESTAMP)  
FROM finance.asset_reconciliation_sap_invoice_import 
;