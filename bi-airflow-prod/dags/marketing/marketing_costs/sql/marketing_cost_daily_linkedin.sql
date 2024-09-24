--linkedin cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_linkedin_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_linkedin
USING staging.marketing_linkedin_supermetric cur
WHERE marketing_cost_daily_linkedin.date::DATE = cur.date::DATE AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_linkedin
SELECT
  date::TIMESTAMP
  ,campaign_name
  ,campaign_group_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
  ,campaign_id::BIGINT
FROM staging.marketing_linkedin_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;

END TRANSACTION;
