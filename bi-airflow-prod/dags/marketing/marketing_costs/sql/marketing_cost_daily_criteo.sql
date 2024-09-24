--criteo cost
BEGIN TRANSACTION;

DELETE FROM staging.marketing_criteo_supermetric
WHERE coalesce(date,'') = '';

DELETE FROM marketing.marketing_cost_daily_criteo
USING staging.marketing_criteo_supermetric cur
WHERE marketing_cost_daily_criteo.date::date = cur.date::date;

INSERT INTO marketing.marketing_cost_daily_criteo
SELECT
  date::TIMESTAMP
  ,account_name
  ,campaign_name
  ,ad_set_name
  ,cost::DOUBLE PRECISION
  ,clicks::BIGINT
  ,impressions::BIGINT
  ,campaign_id::BIGINT
FROM staging.marketing_criteo_supermetric
WHERE date IS NOT NULL;

END TRANSACTION;
