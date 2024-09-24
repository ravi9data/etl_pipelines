/*The aim of this mapping table is to break Subscription into multiple time periods and the corresponding subscription value of the time period, 
in a subscription cycle.A subscription can have 1 or more subscription value (due to plan switching), therefore it is important to deduce the correct value 
with a start and end date. If the subscription is active, it will have no end date. This table will be used as the underlying data source for the 
Datamarts that requires ASV historically*/

CREATE TEMP TABLE tmp_free_year_offer_subscriptions AS 
WITH basis AS (
	SELECT 
		subscription_id,
		date,
		COALESCE(subscription_value_euro, subscription_value) AS sub_value_euro,
		lag(sub_value_euro) OVER (PARTITION BY subscription_id ORDER BY date) AS prev_sub_value_euro,
		subscription_value,
		cancellation_date::date AS cancellation_date  
	FROM master.subscription_historical sh 
	WHERE country_name <> 'United States'
)
, raw_ AS (
	SELECT 
		subscription_id,
		date,
		cancellation_date,
		sub_value_euro AS subscription_value_eur,
		subscription_value AS subscription_value_lc
	FROM basis
	WHERE sub_value_euro <> prev_sub_value_euro OR prev_sub_value_euro IS NULL
)
, initial_sub_value AS (
	SELECT 
		r1.subscription_id,
		r1.subscription_value_lc AS subscription_value_lc_original,
		r1.subscription_value_eur AS subscription_value_eur_original
	FROM raw_ r1
	INNER JOIN (SELECT subscription_id, min(date) AS first_date FROM raw_ GROUP BY 1) r2
	  ON r1.subscription_id = r2.subscription_id
	  AND r1.date = r2.first_date
)
, free_year_flag AS (
	SELECT DISTINCT subscription_id
	FROM raw_ 
	WHERE subscription_value_lc = 0
)
SELECT 
	r.subscription_id,
	r.subscription_value_eur,
	r.subscription_value_lc,
	isv.subscription_value_eur_original,
	isv.subscription_value_lc_original,
	CASE WHEN fyf.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END AS free_year_flag,
	r.date AS start_date,
	COALESCE(DATEADD('day', -1, LEAD(r.date) OVER (PARTITION BY r.subscription_id ORDER BY r.date)), r.cancellation_date)::date AS end_date,
	ROW_NUMBER() OVER (PARTITION BY r.subscription_id ORDER BY r.date ASC) AS phase_idx,
  ROW_NUMBER() OVER (PARTITION BY r.subscription_id ORDER BY r.date DESC) AS latest_phase_idx
FROM raw_ r
LEFT JOIN  initial_sub_value isv
  ON r.subscription_id = isv.subscription_id
LEFT JOIN free_year_flag fyf
  ON r.subscription_id = fyf.subscription_id
WHERE free_year_flag IS TRUE 
;


drop table if exists tmp_subscription_phase_mapping_layer;
create temp table tmp_subscription_phase_mapping_layer as 
with FACT_DAYS AS (
	SELECT
		DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
)
,switch_pre AS (
	select subscription_id, 
		date::date as switch_date,
		upgrade_path,
		sub_value_before,
		sub_value_after,
		lag(date) over (partition by subscription_id order by date asc) as previous_date,
		rank() over (partition by subscription_id,date::Date order by date DESC) as day_idx
	from ods_production.subscription_plan_switching
	where delta_in_sub_value != 0
)	
,switch as (
	select 
		*,
		rank() over (partition by subscription_id order by switch_date ASC) as idx,	 
		count(*) over (partition by subscription_id) as switch_count
	from switch_pre where day_idx = 1
)
,sub_mapping AS (
	select 
		s.subscription_id,
		s.start_date,
		s.status,
        s.customer_id,
        c.customer_type, 
        o.new_recurring,
        s.order_id,
        s.country_name, 
        s.store_commercial,
        s.store_label,
		case when  ss.subscription_id is null then s.start_date::date
		when idx = 1 then s.start_date::date
		else previous_date::Date end as start_date_new,
		s.subscription_value,
		case when ss.subscription_id is null then s.subscription_value_euro else ss.sub_value_after end as new_subscription_value,
		case when ss.subscription_id is null then s.subscription_value_euro else ss.sub_value_before end as old_subscription_value,
		case when ss.subscription_id is null then s.subscription_value else ss.sub_value_after end as new_subscription_value_lc,
		case when ss.subscription_id is null then s.subscription_value else ss.sub_value_before end as old_subscription_value_lc,
		s.subscription_plan,  
		s.rental_period, 
		s.category_name,
		s.subcategory_name,
		s.product_sku,
		s.variant_sku,
		s.store_id,
		s.payment_method, 
		ss.switch_date,
		s.cancellation_date,
		sc.cancellation_reason_churn, 
		sc.cancellation_reason_new,
		sa.avg_asset_purchase_price, 
		idx as switch_idx,
		switch_count = idx as is_last_switch,
		case when s.cancellation_date < start_date_new then s.cancellation_date
		when switch_date is not null then switch_date-1
		when s.cancellation_date is null then null end as end_date
	from ods_production.subscription s 
	left join switch ss on s.subscription_id = ss.subscription_id 
	left join ods_production.customer c on c.customer_id = s.customer_id
	left join ods_production.subscription_cancellation_reason sc on sc.subscription_id = s.subscription_id
	left join ods_production.subscription_assets sa on sa.subscription_id = s.subscription_id
	LEFT JOIN ods_production.order_retention_group o  ON s.order_id = o.order_id 	
)
SELECT 
	fact_day, 
	subscription_id,
	start_date,
	status,
	customer_id,
	s.customer_type, 
	new_recurring,
	order_id,
	country_name, 
	store_commercial,
	store_label,
	case when fact_day = switch_date then new_subscription_value_lc
     	 else old_subscription_value_lc end as subscription_value_lc,
	case when fact_day = switch_date then new_subscription_value
     	 else old_subscription_value end as subscription_value_eur,
	s.subscription_plan,  
	s.rental_period, 
	s.category_name,
	s.subcategory_name,
	product_sku,
	variant_sku, 
	s.payment_method, 
	store_id, 
	cancellation_date, 
	cancellation_reason_churn,
	cancellation_reason_new,
	avg_asset_purchase_price,
	switch_idx, 
	row_number() over (partition by s.subscription_id order by fact_day asc) as phase_idx,
	row_number() over (partition by s.subscription_id order by fact_day desc) as latest_phase_idx,
	FALSE AS free_year_offer_taken,
	case when fact_day = switch_date then cancellation_date::date - 1 
	when switch_idx is null then cancellation_date::date - 1 
	else end_date  end as end_date
from fact_days f
inner join sub_mapping as s
   on f.fact_day::date = s.start_date_new::date or 
	   (f.fact_day::date = switch_date and is_last_switch is true)
WHERE NOT EXISTS (SELECT NULL FROM tmp_free_year_offer_subscriptions t WHERE t.subscription_id = s.subscription_id)
	UNION 
SELECT 
	t.start_date AS fact_day,
	t.subscription_id,
	s.start_date,
	s.status,
	s.customer_id,
	c.customer_type, 
	o.new_recurring,
	s.order_id,
	s.country_name, 
	s.store_commercial,
	s.store_label,
	t.subscription_value_lc,
	t.subscription_value_eur,
	s.subscription_plan,  
	s.rental_period, 
	s.category_name,
	s.subcategory_name,
	product_sku,
	variant_sku, 
	s.payment_method, 
	store_id, 
	s.cancellation_date, 
	cancellation_reason_churn,
	cancellation_reason_new,
	avg_asset_purchase_price,
	NULL AS switch_idx, 
	t.phase_idx,
	t.latest_phase_idx,
	CASE WHEN t.latest_phase_idx = 1 and subscription_value_lc = 0 THEN TRUE ELSE FALSE END free_year_offer_taken,
	t.end_date::timestamp
FROM tmp_free_year_offer_subscriptions t  
LEFT JOIN ods_production.subscription s 
	ON s.subscription_id = t.subscription_id
LEFT JOIN ods_production.customer c 
	ON c.customer_id = s.customer_id
LEFT JOIN ods_production.subscription_cancellation_reason sc 
	ON sc.subscription_id = s.subscription_id
LEFT JOIN ods_production.subscription_assets sa 
	ON sa.subscription_id = s.subscription_id
LEFT JOIN ods_production.order_retention_group o
	ON s.order_id = o.order_id
;



/* Mapping US Subscriptions separately*/


drop table if exists tmp_subscription_phase_mapping_us;
create temp table tmp_subscription_phase_mapping_us as 
with a as (
	select 
		* 
	from tmp_subscription_phase_mapping_layer
	where country_name = 'United States'
	and store_label = 'Grover - United States online'
)
, fx as (
	select 
		dateadd('day',1,date_) as fact_month,
		date_,
		currency,
		exchange_rate_eur
	from trans_dev.daily_exchange_rate  er
	left join public.dim_dates dd on er.date_ = dd.datum 
	where day_is_last_of_month
		and fact_month>= '2022-07-01'
)
select
	case 
		WHEN fact_day > start_date AND date_trunc('month',start_date) = date_trunc('month',fact_month)  THEN fact_day::date
		when date_trunc('month',start_date) = fact_month then start_date::date
		WHEN fact_day <> fact_month AND  date_trunc('month',fact_day) = fact_month THEN fact_day::date
		when fact_month >= '2022-08-01' then fact_month 
		else fact_day end as fact_day
	,subscription_id
	,start_date
	,status
	,customer_id
	,customer_type
	,new_recurring
	,order_id
	,country_name
	,store_commercial
	,store_label
	--,er.exchange_rate_eur as start_date_rate
	,subscription_value_lc
	,case when fact_month >= '2022-08-01' then (subscription_value_lc * fx.exchange_rate_eur)::decimal(10,2)
	    when fact_month < '2022-08-01' then (subscription_value_lc * er.exchange_rate_eur)::decimal(10,2)
	    else subscription_value_eur end as subscription_value_eur
	,subscription_plan
	,rental_period
	,category_name
	,subcategory_name
	,product_sku
	,variant_sku
	,payment_method
	,store_id
	,cancellation_date
	,cancellation_reason_churn
	,cancellation_reason_new
	,avg_asset_purchase_price
	,switch_idx
	,row_number() over (partition by subscription_id order by 
							(case when date_trunc('month',start_date) = fact_month then start_date::date
								  when fact_month >= '2022-08-01' then fact_month 
								  else fact_day end) --fact_day
					 	asc) as phase_idx
	,row_number() over (partition by subscription_id order by 
							(case when date_trunc('month',start_date) = fact_month then start_date::date
								  when fact_month >= '2022-08-01' then fact_month 
								  else fact_day end) --fact_day
						desc) as latest_phase_idx
	,free_year_offer_taken
	,case when end_date is not null then 
			case when date_trunc('month',end_date) = fact_month then end_date
			when fact_month < end_date::date and country_name = 'United States' and date_trunc('month',current_date) = date_trunc('month',fact_month) then current_date 
			when fact_month < end_date::date and country_name = 'United States' then dateadd('day',-1,dateadd('month',1,fact_month))
			else end_date end 
	 when country_name = 'United States' and date_trunc('month',current_date) = date_trunc('month',fact_month) then null
	  when country_name = 'United States' then dateadd('day',-1,dateadd('month',1,fact_month)) end as end_date
from a 
left join fx 
	on fx.fact_month < coalesce(end_date::date,current_timestamp)
	and  fx.fact_month >= date_trunc('month',a.fact_day)
left join  trans_dev.daily_exchange_rate  er 
	on a.start_date::date = er.date_ 
;

/* For all non US Subscriptions separately*/

drop table if exists tmp_subscription_phase_mapping_europe;
create temp table tmp_subscription_phase_mapping_europe as 
select * from tmp_subscription_phase_mapping_layer spm
where  store_label != 'Grover - United States online' ;


drop table if exists ods_production.subscription_phase_mapping;
create table ods_production.subscription_phase_mapping as 
select * from tmp_subscription_phase_mapping_europe
union all 
select * from tmp_subscription_phase_mapping_us;

DROP TABLE tmp_subscription_phase_mapping_layer;
DROP TABLE tmp_subscription_phase_mapping_us;
DROP TABLE tmp_subscription_phase_mapping_europe;

GRANT SELECT ON ods_production.subscription_phase_mapping TO redash_pricing;
GRANT SELECT ON ods_production.subscription_phase_mapping TO tableau;
GRANT SELECT ON ods_production.subscription_phase_mapping TO b2b_redash;
GRANT SELECT ON ods_production.subscription_phase_mapping TO redash_commercial;
