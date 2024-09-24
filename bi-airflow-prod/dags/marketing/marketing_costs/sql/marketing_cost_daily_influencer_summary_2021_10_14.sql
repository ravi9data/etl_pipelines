--influencer cost

DELETE FROM marketing.marketing_cost_daily_influencer_summary_2021_10_14 
USING staging.marketing_influencer_cost_weekly cur
WHERE marketing_cost_daily_influencer_summary_2021_10_14.week_date::DATE = cur.week_date::DATE;

INSERT INTO marketing.marketing_cost_daily_influencer_summary_2021_10_14
SELECT
	country,
	customer_type,
	week_date::TIMESTAMP,
	replace(total_spent_local_currency,',','')::DOUBLE PRECISION,
	currency,
	campaign_name
FROM staging.marketing_influencer_cost_weekly;
