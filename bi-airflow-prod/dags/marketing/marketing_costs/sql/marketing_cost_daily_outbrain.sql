--outbrain cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_outbrain_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_outbrain
USING staging.marketing_outbrain_supermetric cur
WHERE marketing_cost_daily_outbrain.date::DATE = cur.date::DATE
  AND cur.date IS NOT NULL AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_outbrain
SELECT
  date::TIMESTAMP
  ,campaign_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::DOUBLE PRECISION
FROM staging.marketing_outbrain_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;

END TRANSACTION;
