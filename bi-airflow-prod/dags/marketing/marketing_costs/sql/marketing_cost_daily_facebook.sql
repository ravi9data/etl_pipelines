--facebook cost

DELETE FROM staging.marketing_facebook_supermetric
WHERE COALESCE(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_facebook
USING staging.marketing_facebook_supermetric cur
WHERE marketing_cost_daily_facebook.date::DATE = cur.date::DATE
  AND cur.date IS NOT NULL AND cur.campaign_name IS NOT NULL;

INSERT INTO marketing.marketing_cost_daily_facebook
SELECT
   date::TIMESTAMP
  ,account
  ,campaign_name
  ,medium
  ,ad_set_name
  ,ad_name
  ,impressions::BIGINT
  ,clicks::BIGINT
  ,total_spent_local_currency::DOUBLE PRECISION
  ,total_spent_eur::DOUBLE PRECISION
  ,conversions::BIGINT
  ,campaign_id::BIGINT
FROM staging.marketing_facebook_supermetric
WHERE date IS NOT NULL
	AND campaign_name IS NOT NULL;
