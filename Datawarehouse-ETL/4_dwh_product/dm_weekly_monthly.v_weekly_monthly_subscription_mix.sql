CREATE OR REPLACE VIEW dm_weekly_monthly.v_weekly_monthly_subscription_mix AS 
--weekly and monthly -- subscription mix
SELECT 
	'Weekly'::varchar AS report,
	'Acquired Subscriptions'::varchar AS measure,
	date_trunc('week', s.start_date)::date AS date,
	s.rental_period,
	count(DISTINCT s.subscription_id) AS value
FROM master.subscription s
WHERE s.rental_period IN (1,3,6,12,18,24)
	AND date_trunc('week', s.start_date)::date >= date_trunc('week',dateadd('week',-6,current_date))
GROUP BY 1,2,3,4
--
UNION
--
SELECT 
	'Weekly'::varchar AS report,
	'Active Subscriptions'::varchar AS measure,
	date_trunc('week', s.date)::date AS date,
	s.rental_period,
	count(DISTINCT s.subscription_id) AS value
FROM master.subscription_historical s  
LEFT JOIN public.dim_dates d 
	ON s.date = d.datum
WHERE (d.week_day_number = 7 
	OR s.date = current_date -1)
	AND date_trunc('week', s.date)::date >= date_trunc('week',dateadd('week',-6,current_date))
	AND s.status = 'ACTIVE'
	AND s.rental_period IN (1,3,6,12,18,24)
GROUP BY 1,2,3,4
--
UNION 
--
SELECT 
	'Monthly'::varchar AS report,
	'Acquired Subscriptions'::varchar AS measure,
	date_trunc('month', s.start_date)::date AS date,
	s.rental_period,
	count(DISTINCT s.subscription_id) AS value
FROM master.subscription s
WHERE s.rental_period IN (1,3,6,12,18,24)
	AND date_trunc('month', s.start_date)::date >= date_trunc('month',dateadd('month',-14,current_date))
GROUP BY 1,2,3,4
--
UNION
--
SELECT 
	'Monthly'::varchar AS report,
	'Active Subscriptions'::varchar AS measure,
	date_trunc('month', s.date)::date AS date, 
	s.rental_period,
	count(DISTINCT s.subscription_id) AS value
FROM master.subscription_historical s  
LEFT JOIN public.dim_dates d 
	ON s.date = d.datum
WHERE (s.date = LAST_DAY(s.date)
	OR s.date = current_date -1)
	AND date_trunc('month', s.date)::date >= date_trunc('month',dateadd('month',-14,current_date))
	AND s.status = 'ACTIVE'
	AND s.rental_period IN (1,3,6,12,18,24)
GROUP BY 1,2,3,4
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_weekly_monthly.v_weekly_monthly_subscription_mix TO tableau;
