--sponsorships cost

DELETE FROM marketing.marketing_cost_daily_sponsorships 
USING staging.marketing_sponsorships_cost cur
WHERE marketing_cost_daily_sponsorships.date::DATE = cur.date::DATE;

INSERT INTO marketing.marketing_cost_daily_sponsorships
SELECT
	date::TIMESTAMP,
	country,
	replace(replace(cost,',',''),'€','')::DOUBLE PRECISION,
	currency,
	voucher,
	campaign_name
FROM staging.marketing_sponsorships_cost;
