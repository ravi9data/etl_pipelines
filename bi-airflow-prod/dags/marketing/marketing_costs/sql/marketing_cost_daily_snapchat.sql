--snapchat cost

DELETE FROM staging.marketing_snapchat_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_snapchat
USING staging.marketing_snapchat_supermetric cur
WHERE marketing_cost_daily_snapchat.date::DATE = cur.date::DATE AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_snapchat
SELECT
  date::TIMESTAMP
  ,campaign_name
  ,ad_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
FROM staging.marketing_snapchat_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;
