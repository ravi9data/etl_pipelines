DROP VIEW IF EXISTS marketing.v_facebook_cost_daily_with_invoice_credit;
CREATE VIEW marketing.v_facebook_cost_daily_with_invoice_credit AS
SELECT
  a."date"
  ,'Facebook' AS channel
  ,a.account
  ,NULL AS advertising_channel_type
  ,NULL AS campaign_group_name
  ,NULL AS country
  ,NULL AS customer_type
  ,a.campaign_name
  ,a.campaign_id
  ,a.medium
  ,a.ad_name 
  ,a.ad_set_name
  ,CASE
    WHEN a.campaign_name ILIKE '%test_%' THEN TRUE
    ELSE FALSE
   END AS is_test_and_learn
  ,NULL AS brand_non_brand
  ,NULL AS cash_percentage
  ,NULL AS non_cash_percentage
  ,SUM(a.impressions) AS impressions
  ,SUM(a.clicks) AS clicks
  ,SUM(a.total_spent_local_currency) - (SUM(a.total_spent_local_currency) / SUM(a.total_spent_local_currency) OVER(PARTITION BY date_trunc('month', a."date"), a.account)::FLOAT * COALESCE(CASE WHEN a.account = 'Grover US' THEN b.credit_dol ELSE b.credit_eur END,0)) AS total_spent_local_currency
  ,SUM(a.total_spent_eur) - (SUM(a.total_spent_eur) / SUM(a.total_spent_eur) OVER(PARTITION BY date_trunc('month', a."date"), a.account)::FLOAT * COALESCE(b.credit_eur,0)) AS total_spent_eur
  ,SUM(a.conversions) AS conversions
FROM marketing.marketing_cost_daily_facebook a
    LEFT JOIN marketing.facebook_invoice_credit b --This table is manually created by the request from BI-5917 ticket to keep all given credit from FB, as it have only one row, and we guess that FB will not give us credit every month, this table should be updated manually if new invoice credit came so we can reduce actual FB spend
      ON a.account = b.account
      AND DATE_TRUNC('month',a."date") = b.invoice_month
GROUP BY 1,2,3,8,9,10,11,12,13, a.total_spent_eur, a.total_spent_local_currency, b.credit_eur, b.credit_dol
WITH NO SCHEMA BINDING; 
