--retail cost

DELETE FROM marketing.marketing_cost_daily_retail 
USING staging.marketing_retail_cost cur
WHERE marketing_cost_daily_retail.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_retail
SELECT
	date::TIMESTAMP,
	country,
	replace(total_spent_local_currency,',','')::DOUBLE PRECISION,
	currency,
	retail_name,
	channel,
	cost_type,
	placement,
	CASE WHEN is_test_and_learn = 'FALSE' THEN FALSE ELSE TRUE END AS is_test_and_learn
FROM staging.marketing_retail_cost;
