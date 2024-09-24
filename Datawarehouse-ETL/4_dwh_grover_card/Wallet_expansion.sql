DROP TABLE IF EXISTS base_subscriptions;
CREATE TEMP TABLE base_subscriptions AS ( 
WITH max_subs_pre AS (
SELECT fact_date ,
		customer_id,
		sum(active_subscriptions) AS active_subscriptions
		FROM ods_finance.active_subscriptions_overview aso 
		where aso.store_country ='Germany'
		AND aso.customer_type='normal_customer'
		group by 1,2)
,max_subs AS (
	SELECT 
		msp.customer_id ,
		min(first_asset_delivery_date) AS customer_acquisition_cohort,
		max(active_subscriptions) AS max_active_subscriptions
	FROM max_subs_pre msp
	LEFT JOIN master.subscription s 
		ON s.customer_id =msp.customer_id
	GROUP BY 1
	HAVING max_active_subscriptions=3)
	select 
		fact_date,
		aso.customer_id,
		max_active_subscriptions,
		customer_acquisition_cohort,
		c.first_card_created_date ,
		sum(aso.active_subscriptions) as active_subscriptions,
		sum(aso.active_subscription_value) as active_subscription_value
		FROM ods_finance.active_subscriptions_overview aso 
		LEFT JOIN max_subs ms 
			ON ms.customer_id=aso.customer_id 
			ON	c.customer_id =ms.customer_id
		where aso.store_country ='Germany'
		AND aso.customer_type='normal_customer'
		AND max_active_subscriptions IS NOT NULL 
		group by 1,2,3,4,5
		);
		
DROP TABLE IF EXISTS subscription_cross_joins;
CREATE TEMP TABLE subscription_cross_joins AS ( 	
WITH dates AS (
	SELECT
		datum
	FROM
		public.dim_dates dd
	WHERE
		datum <= current_date
  ),
IDs AS
(
	SELECT
		DISTINCT customer_id, 
		customer_acquisition_cohort,
		first_card_created_date,
		max_active_subscriptions,
		min(fact_date) AS min_date
	FROM
		base_subscriptions AS t
	GROUP BY customer_id,customer_acquisition_cohort,max_active_subscriptions,first_card_created_date)
SELECT
	dates.datum AS fact_date,
	i.customer_id,
	i.customer_acquisition_cohort,
	i.max_active_subscriptions,
	i.first_card_created_date,
	CASE WHEN i.customer_acquisition_cohort >i.first_card_created_date THEN 'Acquired_after_card'
		WHEN i.customer_acquisition_cohort <i.first_card_created_date OR (i.customer_acquisition_cohort IS NOT NULL OR i.first_card_created_date IS NULL)
			THEN 'Acquired_before_card'
		END acquired_status,
	COALESCE(t.active_subscriptions, 0) AS active_subscriptions,
	COALESCE(t.active_subscription_value, 0) AS active_subscription_value
FROM
	dates
CROSS JOIN IDs i
LEFT OUTER JOIN base_subscriptions AS t
ON t.fact_date = dates.datum
  AND i.customer_id = t.customer_id
 WHERE dates.datum >= i.min_date
 );

WITH self_join AS (
		SELECT 
			a1.*,
			(a1.active_subscriptions||'->'|| LEAD(a1.active_subscriptions)OVER (PARTITION BY a1.customer_id ORDER BY a1.fact_Date))::VARCHAR  AS upgrade,
			datediff('day',a1.fact_Date,LEAD(a1.Fact_date)OVER (PARTITION BY a1.customer_id ORDER BY a1.fact_Date)) AS no_of_days,
			CASE WHEN a1.active_subscriptions<>a2.active_subscriptions
			 OR a2.active_subscriptions IS NULL  THEN TRUE ELSE FALSE END AS checks
		FROM subscription_cross_joins a1
		LEFT JOIN subscription_cross_joins a2 
		ON a1.customer_id=a2.customer_id
		AND a1.fact_date=dateadd('day',1,a2.fact_date)
		WHERE checks=TRUE
	),pivot_ AS (
		SELECT * FROM (Select customer_id,acquired_status,customer_acquisition_cohort,no_of_days,upgrade, max_active_subscriptions
			 FROM self_join )
		pivot (sum(no_of_days) FOR upgrade IN ('1->1','1->2','2->2','2->3')
		)
		)
,FINAL AS(
	SELECT
		customer_id,
		acquired_status,
		date_trunc('quarter',customer_acquisition_cohort) AS customer_acquisition_cohort_quarter,
		CASE WHEN max_active_subscriptions>1 THEN COALESCE("1->1",0)+COALESCE("1->2",0) ELSE COALESCE("1->2",0) END AS "1-->2",
		CASE WHEN max_active_subscriptions>2 then COALESCE("2->2",0)+COALESCE("2->3",0) ELSE COALESCE("2->3",0) END AS "2-->3" 
	FROM pivot_
	)
SELECT 
	customer_acquisition_cohort_quarter,
	acquired_status,
	count(customer_id),
	round(sum("1-->2")/count(customer_id)::decimal,0) AS "1-->2",
	round(sum("2-->3")/count(customer_id)::decimal,0) AS "2-->3"
FROM FINAL 
GROUP BY 1,2
)
;

