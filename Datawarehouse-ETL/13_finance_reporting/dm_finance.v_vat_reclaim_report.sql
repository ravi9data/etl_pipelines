DROP VIEW IF EXISTS dm_finance.v_vat_reclaim;
CREATE VIEW dm_finance.v_vat_reclaim AS 
SELECT * 
FROM finance.vat_reclaim_report_eoy_snapshots 
UNION ALL 
SELECT DISTINCT
 CURRENT_DATE AS "date"
 ,sp.customer_id
 ,sp.asset_id
 ,s.subscription_sf_id 
 ,s.subscription_bo_id
 ,s.subscription_id
 ,sp.payment_id
 ,sp.payment_sfid AS salesforce_payment_id
 ,sp.currency
 ,sp.country_name
 ,sp.tax_rate 
 ,sp.due_date::DATE AS due_date
 ,sp.invoice_date::DATE AS invoice_date
 ,sp.amount_due AS amount_due
 ,NULL AS amount_without_tax
 ,NULL AS amount_vat_tax
 ,sp.amount_chargeback AS amount_chargeback
 ,CASE
   WHEN sp.amount_chargeback > sp.amount_due 
    THEN 'CHARGED BACK'
   ELSE sp.status
  END AS PAYMENT_STATUS
  ,s.cancellation_date::DATE AS cancellation_date
  ,s.cancellation_reason AS cancellation_reason
  ,a.capital_source_name
  ,sp.invoice_url 
FROM master.subscription_payment sp
  LEFT JOIN master.subscription s 
    ON  s.subscription_id = sp.subscription_id
  LEFT JOIN master.asset a 
    ON  a.asset_id = sp.asset_id
WHERE TRUE 
  AND (sp.status NOT IN ('PAID', 'CANCELLED', 'HELD', 'PLANNED', 'PENDING')
       OR COALESCE(sp.amount_chargeback, 0) > 0)
 AND sp.subscription_id IS NOT NULL
 AND s.delivered_assets >= 1
 WITH NO SCHEMA BINDING;

 GRANT SELECT ON dm_finance.v_vat_reclaim TO tableau;