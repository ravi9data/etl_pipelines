--partnership cost
DELETE FROM marketing.marketing_cost_daily_partnership 
USING staging.marketing_partnership_cost cur
WHERE marketing_cost_daily_partnership.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_partnership
SELECT
	date::TIMESTAMP,
	country,
	replace(replace(total_spent_local_currency,',',''),'€','')::DOUBLE PRECISION,
	currency,
	partner_name,
	cost_type,
	CASE WHEN is_test_and_learn = 'FALSE' THEN FALSE ELSE TRUE END AS is_test_and_learn,
	campaign_name
FROM staging.marketing_partnership_cost;
