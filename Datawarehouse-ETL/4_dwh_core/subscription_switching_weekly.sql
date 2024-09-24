DROP TABLE IF EXISTS dm_weekly_monthly.subscription_switching;
CREATE TABLE dm_weekly_monthly.subscription_switching AS
SELECT 
	date_trunc('week', sps.date) as reporting_date, 
	duration_before as initial_duration, 
	count(sps.subscription_id) as subscriptions,
	'weekly' as reporting
FROM ods_production.subscription_plan_switching sps 
WHERE TRUE
  AND date <= CURRENT_DATE
  AND date >= DATE_TRUNC('week', DATEADD('week', -13, CURRENT_DATE))
  AND sps.duration_before IN ('1','3', '6', '12','18')
group BY 1,2
	union all
SELECT 
	date_trunc('month', sps.date) as reporting_date, 
	duration_before as initial_duration, 
	count(sps.subscription_id) as subscriptions,
	'monthly' as reporting
FROM ods_production.subscription_plan_switching sps 
WHERE TRUE
  AND date <= CURRENT_DATE
  AND date >= DATE_TRUNC('MONTH', DATEADD('month', -13, CURRENT_DATE))
group BY 1,2;

GRANT SELECT ON dm_weekly_monthly.subscription_switching TO tableau;
