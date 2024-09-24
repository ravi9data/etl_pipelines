--reddit cost

DELETE FROM marketing.marketing_cost_daily_reddit 
USING staging.marketing_reddit_cost cur
WHERE marketing_cost_daily_reddit.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_reddit
SELECT
	date::TIMESTAMP,
	country,
	campaign_name,
	replace(replace(costs,',',''),'€','')::DOUBLE PRECISION AS cost,
	currency
FROM staging.marketing_reddit_cost;
