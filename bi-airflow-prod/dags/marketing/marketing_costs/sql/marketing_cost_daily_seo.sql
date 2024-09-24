--seo cost

DELETE FROM marketing.marketing_cost_daily_seo 
USING staging.marketing_seo_cost cur
WHERE marketing_cost_daily_seo.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_seo
SELECT
	date::TIMESTAMP,
	country,
	replace(total_spent_local_currency,',','')::BIGINT,
	currency,
	url,
	CASE WHEN is_test_and_learn = 'FALSE' THEN FALSE ELSE TRUE END AS is_test_and_learn
FROM staging.marketing_seo_cost;
