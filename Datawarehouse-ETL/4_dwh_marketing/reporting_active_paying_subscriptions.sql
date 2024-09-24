--basically we are going to work with incremental data in active subs. 
--And with paid subs, we will reload everything, since these metrics can change
DROP TABLE IF EXISTS tmp_reporting_active_paying_subscriptions_active_subs;
CREATE TEMP TABLE tmp_reporting_active_paying_subscriptions_active_subs
SORTKEY(fact_day, customer_type)
DISTKEY(fact_day)
AS
WITH FACT_DAYS AS (
	SELECT DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
		AND datum >= dateadd('day',-3,current_date)
)
	 	SELECT
	 		f.fact_day,
			ss.customer_type,
	 		coalesce(sum(s.subscription_value_eur),0) AS total_volume,
	 		coalesce(count(distinct s.subscription_id),0) AS total_subscriptions,
	 		coalesce(count(distinct s.customer_id),0) AS total_customers
	 	FROM FACT_DAYS f 
	 	LEFT JOIN ods_production.subscription_phase_mapping s
	 		ON f.fact_day::date >= s.fact_Day::date 
			AND F.fact_day::date <= coalesce(s.end_date::date, f.fact_day::date+1)
		LEFT JOIN master.subscription ss
			ON s.subscription_id = ss.subscription_id
		GROUP BY 1,2
;

DROP TABLE IF EXISTS tmp_reporting_active_paying_subscriptions_paid_subs;
CREATE TEMP TABLE tmp_reporting_active_paying_subscriptions_paid_subs
SORTKEY(fact_day, customer_type)
DISTKEY(fact_day)
AS
WITH FACT_DAYS AS (
	SELECT DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
)
		SELECT 
			f.fact_day,
			ss.customer_type,
			coalesce(sum(s.subscription_value_eur),0) AS paid_volume,
			coalesce(count(distinct s.subscription_id),0) AS paid_subscriptions,
			coalesce(count(distinct s.customer_id),0) AS paid_customers
		from FACT_DAYS f 
	 	LEFT JOIN ods_production.subscription_phase_mapping s
	 		ON f.fact_day::date >= s.fact_Day::date 
			and F.fact_day::date <= coalesce(s.end_date::date, f.fact_day::date+1)
		LEFT JOIN master.subscription ss
			ON s.subscription_id = ss.subscription_id
		WHERE NOT EXISTS 
			(SELECT 1 
 			FROM ods_production.payment_subscription sp 
 			WHERE s.subscription_id = sp.subscription_id
				AND sp.status IN ('FAILED','FAILED FULLY','NOT PROCESSED'))
		GROUP BY 1,2
;

BEGIN TRANSACTION;

DELETE FROM dwh.reporting_active_paying_subscriptions
USING tmp_reporting_active_paying_subscriptions_active_subs tmp
WHERE reporting_active_paying_subscriptions.fact_day::date = tmp.fact_day::date ;

INSERT INTO dwh.reporting_active_paying_subscriptions
SELECT * 
FROM tmp_reporting_active_paying_subscriptions_active_subs;

END TRANSACTION;


DROP TABLE IF EXISTS tmp_reporting_active_paying_subscriptions;
CREATE TEMP TABLE tmp_reporting_active_paying_subscriptions
SORTKEY(fact_day, customer_type)
DISTKEY(fact_day)
AS
	SELECT
		COALESCE(a.fact_day, p.fact_day) AS fact_day,
		COALESCE(a.customer_type, p.customer_type) AS customer_type,
		a.total_volume,
		a.total_subscriptions,
		a.total_customers,
		p.paid_volume,
		p.paid_subscriptions,
		p.paid_customers
	FROM dwh.reporting_active_paying_subscriptions a 
	FULL JOIN tmp_reporting_active_paying_subscriptions_paid_subs p 
		ON a.fact_day = p.fact_day 
		AND a.customer_type = p.customer_type
;


BEGIN TRANSACTION;

DROP TABLE IF EXISTS dwh.reporting_active_paying_subscriptions;
CREATE TABLE dwh.reporting_active_paying_subscriptions AS
	SELECT
	    *
	FROM tmp_reporting_active_paying_subscriptions;
 
END TRANSACTION;

GRANT SELECT ON dwh.reporting_active_paying_subscriptions TO tableau;
