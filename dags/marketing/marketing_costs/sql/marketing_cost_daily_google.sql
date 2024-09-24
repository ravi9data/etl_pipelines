--google cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_google_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_google
USING staging.marketing_google_supermetric cur
WHERE marketing_cost_daily_google.date::DATE = cur.date::DATE
  AND cur.date IS NOT NULL AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_google
SELECT
  date::TIMESTAMP
  ,account
  ,advertising_channel_type
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::DOUBLE PRECISION
  ,campaign_id::BIGINT
FROM staging.marketing_google_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;

END TRANSACTION;
