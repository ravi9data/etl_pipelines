--tiktok cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_tiktok_supermetric
WHERE coalesce(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_tiktok
USING staging.marketing_tiktok_supermetric cur
WHERE marketing_cost_daily_tiktok.date::date = cur.date::date AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_tiktok
SELECT
  date::TIMESTAMP
  ,campaign_name
  ,ad_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
  ,campaign_id::BIGINT
FROM staging.marketing_tiktok_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;

END TRANSACTION;
