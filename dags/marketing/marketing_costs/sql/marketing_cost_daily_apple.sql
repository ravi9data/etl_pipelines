--apple cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_apple_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_apple
USING staging.marketing_apple_supermetric cur
WHERE marketing_cost_daily_apple.date::DATE = cur.date::DATE
    AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_apple
SELECT
  date::TIMESTAMP
  ,account
  ,campaign_name
  ,impressions::BIGINT
  ,installs::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,campaign_id::BIGINT
FROM staging.marketing_apple_supermetric
WHERE campaign_name IS NOT NULL;

END TRANSACTION;
