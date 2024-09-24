--affiliate fixed cost

DELETE FROM marketing.marketing_cost_daily_affiliate_fixed 
USING staging.marketing_affiliate_fixed_costs cur
WHERE marketing_cost_daily_affiliate_fixed.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_affiliate_fixed
SELECT
	date::TIMESTAMP,
	country,
	replace(total_spent_local_currency,',','')::DOUBLE PRECISION,
	currency,
	affiliate_network,
	affiliate
FROM staging.marketing_affiliate_fixed_costs;
