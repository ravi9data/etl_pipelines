WITH first_asset AS (
	SELECT 
		date_trunc('month',first_card_created_date)::date AS card_created_date,
		cs.customer_id ,
		min(first_asset_delivery_date) AS first_asset_delivery_date
		WHERE first_card_created_date IS NOT NULL 
		--aND CUSTOMER_ID='35383'
		GROUP BY 1,2)
,sub_base AS (
	SELECT card_created_date,
		date_trunc('month',start_date)::date AS subscription_start_date,
		fa.first_asset_delivery_date::Date,
		cs.subscription_id  AS acquired_subs,
		cs.customer_id ,
		ROW_NUMBER()OVER(PARTITION BY card_created_date,cs.customer_id ORDER BY card_created_date,start_date) idx
	FROM first_asset fa 
		ON fa.customer_id=cs.customer_id 
	WHERE first_card_created_date IS NOT NULL 
	AND fa.first_asset_delivery_date>= first_card_created_date
	--AND cs.customer_id ='941649'
	ORDER BY 1,2
	)
,subs AS (
SELECT * FROM sub_base WHERE idx=1	
)
,a AS (
	SELECT cc.customer_id ,
		date,
		cc.first_card_created_date::date,
--		sh.subscription_id,
--		sh.start_date ,
		first_value(crm_label)OVER (PARTITION BY ch.customer_id) crm_label_before,
		nth_value(crm_label,2)OVER (PARTITION BY ch.customer_id) crm_label_after
	FROM master.customer_historical ch
				WHERE first_card_created_date IS NOT NULL ) cc
	ON ch.customer_id=cc.customer_id
WHERE true
AND ch.date=last_day(ch.date)
AND (ch.date=date_trunc('month',first_card_created_date)-1 OR ch.date=last_Day(first_card_created_date))
)
, b AS (
SELECT a.*,sh.subscription_id ,sh.start_date ,
ROW_NUMBER()OVER(PARTITION BY a.customer_id ORDER BY a.date DESC,start_date desc ) AS idx
FROM a 
LEFT JOIN master.subscription_historical sh 
ON sh.customer_id=a.customer_id 
		AND sh.date=a.date 
		AND date_trunc('month',start_date)::date= date_trunc('month',a.first_card_created_date)::date	
)
,reactivation AS (
	SELECT DISTINCT 
	date_trunc('month',first_card_created_date)::date AS card_created_date,
	date_trunc('month',start_date)::date AS subscription_start_date,
	b.customer_id AS reactivated_customers
	FROM b
		WHERE idx=1 
		AND subscription_id IS NOT NULL 
		AND crm_label_before='Inactive'
		AND crm_label_after='Active'	
		)
SELECT  
	s.card_created_date,
	NULL::Date AS subscription_start_date,
	count(DISTINCT reactivated_customers) AS reactivated_customers,
	count(DISTINCT acquired_subs) AS acquired_cusotmers
FROM subs s
FULL OUTER JOIN reactivation r 
ON s.card_created_date::date=r.card_created_date::date
GROUP BY 1,2
UNION all
SELECT  
	NULL::date AS card_created_date,
	s.subscription_start_date,
	count(DISTINCT reactivated_customers) AS reactivated_customers,
	count(DISTINCT acquired_subs) AS acquired_cusotmers
FROM subs s
FULL OUTER JOIN reactivation r
ON s.subscription_start_date::date=r.subscription_start_date::date
WHERE s.subscription_start_date>='2021-04-01'
GROUP BY 1,2
)
WITH NO SCHEMA binding;


