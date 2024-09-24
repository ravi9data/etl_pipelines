DROP TABLE IF EXISTS marketing.marketing_cost_daily_combined;
CREATE TABLE marketing.marketing_cost_daily_combined AS
WITH cash_non_cash AS (
SELECT DISTINCT cash_non_cash
FROM marketing.marketing_cost_channel_mapping mccm
)
SELECT
  a.date::date AS reporting_date
  ,mccm.channel_grouping
  ,COALESCE(mccm.channel_detailed, mccm.channel, a.channel) AS channel
  ,mccm.country
  ,mccm.customer_type
  ,COALESCE(a.brand_non_brand, mccm.brand_non_brand) AS brand_non_brand
  ,COALESCE(c.cash_non_cash, mccm.cash_non_cash) AS cash_non_cash
  ,mccm.reporting_grouping_name
  ,a.campaign_name
  ,a.campaign_id
  ,a.medium
  ,a.ad_set_name
  ,a.ad_name
  ,a.is_test_and_learn
  ,a.impressions
  ,a.clicks
  ,CASE
  	WHEN COALESCE(c.cash_non_cash, mccm.cash_non_cash) = 'Cash'
  	 THEN a.total_spent_local_currency * COALESCE(a.cash_percentage, 1)
  	WHEN COALESCE(c.cash_non_cash, mccm.cash_non_cash) = 'Non-Cash'
  	 THEN a.total_spent_local_currency * COALESCE(a.non_cash_percentage, 1)
  	ELSE a.total_spent_local_currency
  END total_spent_local_currency
  ,CASE
  	WHEN COALESCE(c.cash_non_cash, mccm.cash_non_cash) = 'Cash'
  	 THEN a.total_spent_eur * COALESCE(a.cash_percentage, 1)
  	WHEN COALESCE(c.cash_non_cash, mccm.cash_non_cash)= 'Non-Cash'
  	 THEN a.total_spent_eur * COALESCE(a.non_cash_percentage, 1)
  	ELSE a.total_spent_eur
  END total_spent_eur
  ,a.conversions
FROM marketing.marketing_cost_daily_base_data a
  LEFT JOIN cash_non_cash c
    ON a.channel IN ('TV', 'Billboard')
  LEFT JOIN marketing.marketing_cost_channel_mapping mccm
    ON a.channel = mccm.channel
    AND CASE
      WHEN mccm.account IS NOT NULL
       THEN a.account = mccm.account
      ELSE TRUE
      END
    AND CASE
      WHEN a.country IS NOT NULL
       THEN a.country = mccm.country
      ELSE TRUE
      END
    AND CASE
      WHEN a.customer_type IS NOT NULL
       THEN a.customer_type = mccm.customer_type
      ELSE TRUE
      END
    AND CASE
      WHEN mccm.advertising_channel_type IS NOT NULL
       THEN a.advertising_channel_type = mccm.advertising_channel_type
      ELSE TRUE
     END
    AND CASE
      WHEN mccm.medium IS NOT NULL
       THEN a.medium = mccm.medium
      ELSE TRUE
      END
     AND CASE
    WHEN mccm.campaign_name_contains IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_contains) IN LOWER(a.campaign_name_modified)) > 0
      THEN TRUE
    ELSE FALSE
   END
  AND CASE
    WHEN mccm.campaign_name_contains2 IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_contains2) IN LOWER(a.campaign_name_modified)) > 0
      THEN TRUE
    ELSE FALSE
   END
   AND CASE
    WHEN mccm.campaign_name_does_not_contain IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_does_not_contain) IN LOWER(a.campaign_name_modified)) = 0
      THEN TRUE
    ELSE FALSE
   END
  AND CASE
    WHEN mccm.campaign_group_name_contains IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_group_name_contains) IN LOWER(a.campaign_group_name)) > 0
      THEN TRUE
    ELSE FALSE
   END
  WHERE a.date::DATE < CURRENT_DATE
;

GRANT SELECT ON marketing.marketing_cost_daily_combined TO group data_science;
GRANT SELECT ON marketing.marketing_cost_daily_combined TO tableau;
GRANT SELECT ON marketing.marketing_cost_daily_combined TO hams;
