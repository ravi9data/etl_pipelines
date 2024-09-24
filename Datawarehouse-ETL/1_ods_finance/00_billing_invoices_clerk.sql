DROP TABLE IF EXISTS ods_production.billing_invoices_clerk;
CREATE TABLE ods_production.billing_invoices_clerk AS
WITH last_invoice_pdf AS (
SELECT  
  *
FROM oltp_clerk.invoice_pdf 
WHERE TRUE 
QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(payments_id, invoice_id) ORDER BY created_at DESC) = 1  
)
SELECT 
  i.id AS invoice_id
 ,i.number AS number_
 ,i.number_sequence AS invoice_sequence
 ,pdf.invoice_number AS invoice_number_pdf
 ,pdf.invoice_date
 ,pdf.public_url AS invoice_url
 ,pdf.order_number AS order_id
--  ,subscription_id
--  ,consolidated
 ,pdf.invoice_due_date AS due_date
--  ,pay_by_invoice
 ,pdf.payments_id AS payment_id
 ,i.payment_group_id
 ,i.movement_id
 ,pdf.customer_id AS customer_id
 ,pdf.invoice_start AS billing_period_start
 ,pdf.invoice_end AS billing_period_end
 ,pdf.amount_total AS total_invoice_amount
FROM oltp_clerk.invoice i
  INNER JOIN last_invoice_pdf pdf
    ON i.id = pdf.invoice_id 
WHERE i.type <> 'CREDIT NOTE'    
QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(i.payment_group_id, i.movement_id, pdf.payments_id) ORDER BY i.created_at DESC) = 1  
;