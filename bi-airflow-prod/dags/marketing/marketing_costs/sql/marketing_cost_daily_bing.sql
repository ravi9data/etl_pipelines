--bing cost

DELETE FROM staging.marketing_bing_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_bing
USING staging.marketing_bing_supermetric cur
WHERE marketing_cost_daily_bing.date::DATE = cur.date::DATE AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_bing
SELECT
  date::TIMESTAMP
  ,account
  ,advertising_channel_type
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
  ,campaign_id::BIGINT
FROM staging.marketing_bing_supermetric
WHERE campaign_name IS NOT NULL;
