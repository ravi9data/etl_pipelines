--podcasts cost

DELETE FROM marketing.marketing_cost_daily_podcasts 
USING staging.marketing_podcasts_cost cur
WHERE marketing_cost_daily_podcasts.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_podcasts
SELECT
	date::TIMESTAMP,
	country,
	replace(replace(total_spent_local_currency,',',''),'€','')::DOUBLE PRECISION,
	currency,
	podcast_name,
	CASE WHEN is_test_and_learn = 'FALSE' THEN FALSE ELSE TRUE END AS is_test_and_learn,
	campaign_name
FROM staging.marketing_podcasts_cost;
