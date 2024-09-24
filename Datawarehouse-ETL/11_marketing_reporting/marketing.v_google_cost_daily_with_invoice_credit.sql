DROP VIEW IF EXISTS marketing.v_google_cost_daily_with_invoice_credit;
CREATE VIEW marketing.v_google_cost_daily_with_invoice_credit AS
SELECT
    a."date"
     ,'Google' AS channel
     ,a.account
     ,case when a.advertising_channel_type = 'Discovery' AND a."date" >= '2023-11-10' then 'Demand Gen' ELSE a.advertising_channel_type end AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,a.campaign_name
     ,a.campaign_id
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(a.impressions) AS impressions
     ,SUM(a.clicks) AS clicks
     ,SUM(a.total_spent_local_currency) - (SUM(a.total_spent_local_currency) / NULLIF(SUM(a.total_spent_local_currency) OVER(PARTITION BY CASE WHEN b.account IS NOT NULL THEN TRUE END,CASE WHEN b.advertising_channel_type IS NOT NULL THEN TRUE END,a.account)::FLOAT,0) * COALESCE(CASE WHEN a.account ilike '%Grover US%' THEN b.credit_dol ELSE b.credit_eur END,0)) AS total_spent_local_currency
     ,SUM(a.total_spent_eur) - (SUM(a.total_spent_eur) / NULLIF(SUM(a.total_spent_eur) OVER(PARTITION BY CASE WHEN b.account IS NOT NULL THEN TRUE END,CASE WHEN b.advertising_channel_type IS NOT NULL THEN TRUE END, a.account)::FLOAT,0) * COALESCE(b.credit_eur,0)) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_google a
         LEFT JOIN marketing.google_invoice_credit_and_cost b
                   ON a.account = b.account
                       AND a."date"::DATE BETWEEN b.invoice_start AND b.invoice_end
                       AND CASE WHEN b.advertising_channel_type IS NOT NULL
                                    THEN b.advertising_channel_type = a.advertising_channel_type ELSE TRUE END
                       AND CASE WHEN b.definition IS NOT NULL
                                    THEN CASE WHEN a.campaign_name ILIKE '%brand%' OR a.campaign_name ILIKE '%trademark%'
                                                  THEN 'Brand' ELSE 'Non Brand'
                                             END = b.definition ELSE TRUE END
GROUP BY 1,3,4,8,9, a.total_spent_local_currency, a.total_spent_eur, b.credit_eur, b.credit_dol, b.account, b.advertising_channel_type
WITH NO SCHEMA BINDING;
--This table marketing.google_invoice_credit_and_cost is manually created by the request from BI-6314 and BI-6991 ticket to keep all given credit / additional costs from/to Google, as it have only one row, and we guess that google will not give us credit every month, this table should be updated manually if new invoice credit came so we can reduce/increase actual google spend
--Collumn description is available in table DDL
