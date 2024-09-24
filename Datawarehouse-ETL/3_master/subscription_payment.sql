BEGIN;

TRUNCATE TABLE master.subscription_payment;

INSERT INTO master.subscription_payment
WITH outstanding_payments_adjusted_for_pbi_payments AS (
SELECT DISTINCT
  sp.customer_id
 ,sp.subscription_payment_id AS payment_id
 ,sp.due_date
 ,CASE 
   WHEN o.is_pay_by_invoice IS TRUE 
    THEN DATEADD('day', 14, sp.due_date) 
   ELSE sp.due_date 
  END AS pay_by_invoice_adjusted_due_date
 ,CASE 
   WHEN sp.payment_type = 'FIRST' 
     AND sp.status = 'FAILED' 
    THEN sp.amount_due 
   ELSE 0 
  END AS first_failed_payment_due_amount
 ,CASE 
   WHEN sp.status IN ('PAID', 'NEW') 
    THEN 'PAID or NEW Payments'
   WHEN sp.status IN ('FAILED', 'PENDING') 
     AND pay_by_invoice_adjusted_due_date < CURRENT_DATE 
    THEN 'OVERDUE'
   ELSE 'IN INTERVAL'
  END AS payment_status_adjusted
 ,CASE 
   WHEN sp.due_date > CURRENT_DATE 
    THEN 0
   WHEN sp.paid_date IS NULL 
     AND sp.due_date < CURRENT_DATE 
    THEN sp.amount_due 
   WHEN sp.paid_date IS NOT NULL 
    THEN sp.amount_due - sp.amount_paid
   ELSE 0
  END AS amount_overdue
 ,CASE 
   WHEN o.is_pay_by_invoice IS TRUE 
     AND payment_status_adjusted = 'IN INTERVAL' 
    THEN amount_due 
   ELSE 0 
  END AS pbi_amount_due_adjusted
 ,amount_overdue - first_failed_payment_due_amount - pbi_amount_due_adjusted AS amount_overdue_pbi_adjusted
FROM ods_production.payment_subscription sp 
  LEFT JOIN ods_production.order o 
    ON o.order_id = sp.order_id 
)
,overdue_amount_pbi_adjusted AS (
SELECT DISTINCT 
  payment_id
 ,amount_overdue_pbi_adjusted
 ,due_date
 ,payment_status_adjusted AS payment_status_pbi_adjusted
FROM outstanding_payments_adjusted_for_pbi_payments
)
SELECT DISTINCT
  sp.payment_group_id
 ,sp.subscription_payment_id AS payment_id
 ,sp.subscription_payment_name AS payment_sfid
 ,sp.transaction_id
 ,sp.resource_id
 ,sp.movement_id
 ,sp.psp_reference_id AS psp_reference
 ,sp.customer_id
 ,c.customer_type
 ,sp.order_id
 ,sp.asset_id
 ,a.brand
 ,sp.allocation_id AS allocation_id
 ,a.category_name
 ,a.subcategory_name
 ,sp.subscription_id
 ,CASE 
   WHEN COALESCE(sa.delivered_assets,0) > 0 
    THEN TRUE 
   ELSE FALSE 
  END AS asset_was_delivered,
  CASE 
   WHEN COALESCE(sa.outstanding_assets,0) = 0 
     AND COALESCE(sa.delivered_assets,0) > 0 
    THEN TRUE 
   ELSE FALSE 
  END AS asset_was_returned
 ,sp.created_at
 ,GREATEST(
    sp.updated_at
   ,c.updated_at
   ,s.updated_date
   ,sa.updated_at
   ,sc.updated_at
   ,a.updated_date
   ,pii.updated_at
   ,cs.updated_at
   ,sd.updated_at) AS updated_at
 ,DATE_TRUNC('day',s.start_date) AS subscription_start_date
 ,sp.billing_period_start
 ,sp.billing_period_end
 ,sp.payment_number
 ,sp.payment_type
 ,sp.due_date
 ,sp.paid_date
 ,sp.money_received_at
 ,sp.failed_date
 ,sp.attempts_to_pay
 ,sd.subscription_payment_category
 ,CASE
    WHEN sc.last_valid_payment_category LIKE ('%DEFAULT%')
      AND sc.last_valid_payment_category NOT LIKE ('%RECOVERY%')
      AND asset_was_returned = FALSE
      AND sp.due_date > sd.next_due_date
    THEN 'non_performing'
    WHEN sc.last_valid_payment_category LIKE ('%DEFAULT%')
      AND sc.last_valid_payment_category NOT LIKE ('%RECOVERY%')
      AND sp.due_date = sd.next_due_date
      AND asset_was_returned = FALSE
    THEN 'default_new'
    ELSE 'not_default'
  END AS default_new
 ,sp.status
 ,sp.payment_processor_message
 ,sp.payment_method_details
 ,sp.paid_status
 ,sd.next_due_Date
 ,sd.dpd
 ,sp.payment_method_detailed
 ,sp.currency
 ,sp.amount_due
 ,sp.amount_paid
 ,sp.amount_subscription
 ,sp.amount_shipment
 ,sp.amount_voucher
 ,sp.amount_discount
 ,sp.amount_tax
 ,sp.tax_rate
 ,sp.amount_overdue_fee
 ,sp.refund_amount AS amount_refund
 ,sp.chargeback_amount AS amount_chargeback
 ,CASE 
   WHEN s.debt_collection_handover_date IS NOT NULL 
    THEN s.debt_collection_handover_date
   WHEN sa.debt_collection_assets > 0 
    THEN s.cancellation_date 
  END AS debt_collection_handover_date
 ,s.result_debt_collection_contact
 ,CASE
   WHEN sp.date_debt_collection_handover::DATE IS NOT NULL 
     AND (sp.paid_date>sp.date_debt_collection_handover) 
    THEN TRUE
   ELSE FALSE
  END AS is_dc_collections
 ,sd.is_eligible_for_refund
 ,s.subscription_plan
 ,sp.country_name
 ,s.store_label::VARCHAR(30)
 ,s.store_name
 ,s.store_short
 ,os.file_path
 ,cs.burgel_risk_category::VARCHAR(15)
 ,CASE
   WHEN a.asset_status_original = 'OFFICE' 
    THEN 'is_office_asset'
   WHEN sp.status = 'PLANNED'
     AND sp.due_date < CURRENT_DATE
    THEN 'not_triggered_payment'
   WHEN DATE_TRUNC('month',s.start_date)::DATE = '2017-12-01'
     OR DATE_TRUNC('month',s.start_date)::DATE = '2018-01-01'
    THEN 'sepa_cohort'
   WHEN sp.currency NOT IN ('EUR') 
    THEN 'non_eur_currency'
   ELSE 'ok'
  END AS covenant_exclusion_criteria
 ,sp.invoice_url
 ,sp.invoice_number
 ,sp.invoice_date 
 ,sp.invoice_sent_date
 ,sp.capital_source
 ,pbi.amount_overdue_pbi_adjusted
 ,pbi.payment_status_pbi_adjusted
 ,sp.payment_method
 ,sp.src_tbl
FROM ods_production.payment_subscription sp
  LEFT JOIN ods_production.payment_subscription_details sd
    ON sp.subscription_payment_id=sd.subscription_payment_id
   AND COALESCE(sp.paid_date,'1990-05-22') = COALESCE(sd.paid_date,'1990-05-22')
  LEFT JOIN ods_production.customer c
    ON c.customer_id = sp.customer_id
  LEFT JOIN ods_production.customer_scoring cs
    ON cs.customer_id = sp.customer_id
  LEFT JOIN ods_production.subscription s
    ON s.subscription_id=sp.subscription_id
  LEFT JOIN ods_production.subscription_assets sa
    ON s.subscription_id = sa.subscription_id
  LEFT JOIN ods_production.subscription_cashflow sc
    ON sc.subscription_id=sp.subscription_id
  LEFT JOIN ods_production.asset a
    ON a.asset_id=sp.asset_id
  LEFT JOIN ods_data_sensitive.customer_pii pii
    ON pii.customer_id=sp.customer_id
  LEFT JOIN ods_production.order_scoring os
    ON os.order_id = sp.order_id
  LEFT JOIN overdue_amount_pbi_adjusted pbi
    ON pbi.payment_id = sp.subscription_payment_id
   AND pbi.due_date = sp.due_date 
WHERE TRUE
 AND sp.status::TEXT <> 'CANCELLED'::TEXT
 AND (sp.allocation_id IS NOT NULL OR sp.subscription_id IS NOT NULL)
ORDER BY sp.subscription_id, sp.payment_number
;

COMMIT;
