WITH shares AS (
    SELECT date_trunc('month',event_timestamp) AS reporting_month,
           country,
           currency,
           earned_referral / total_earned::FLOAT as referral_perc,
    WHERE event_name != 'Redemption'
    GROUP BY 1,2,3
),

cost_calculation AS (
SELECT a.event_timestamp::DATE AS reporting_date,
       a.country,
       CASE WHEN a.country = 'United States' THEN 'USD' ELSE a.currency END as currency,
       total_redeemed * referral_perc AS referral_cost,
LEFT JOIN shares b on a.country = b.country and date_trunc('month',a.event_timestamp) = b.reporting_month
WHERE event_name = 'Redemption'

SELECT reporting_date, 
	country,
	currency, 
	referral_cost
FROM cost_calculation
WITH NO SCHEMA BINDING;

