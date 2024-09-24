DROP TABLE IF EXISTS finance.vat_reclaim_report_eoy_snapshots;
CREATE TABLE finance.vat_reclaim_report_eoy_snapshots AS
SELECT DISTINCT
 sp.date
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
FROM master.subscription_payment_historical sp
  LEFT JOIN master.subscription_historical s 
    ON  s.subscription_id = sp.subscription_id
    AND s.date = sp.date
  LEFT JOIN master.asset_historical a 
    ON  a.asset_id = sp.asset_id
    AND a.date = sp.date
WHERE TRUE 
  AND sp.date IN ('2020-12-31', '2021-12-31')
 AND (sp.status NOT IN ('PAID', 'CANCELLED', 'HELD', 'PLANNED', 'PENDING')
       OR COALESCE(sp.amount_chargeback, 0) > 0)
 AND sp.subscription_id IS NOT NULL
 AND s.delivered_assets >= 1
;

GRANT SELECT ON finance.vat_reclaim_report_eoy_snapshots TO tableau;