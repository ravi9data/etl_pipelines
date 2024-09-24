CREATE OR REPLACE VIEW dm_weekly_monthly.v_weekly_cs_reporting as 
with fact_ as (
select distinct 
	date_trunc ('week', datum)::date as fact_week,
	s.country_name as store_country
from public.dim_dates dd 
	left join 
	(select distinct case when s.country_name ='Andorra' then 'n/a' else country_name end as country_name, min(created_date)::date from ods_production.store s 
		where country_name not in ('United Kingdom')
		group by 1) as s 
	on datum >= s.min::date  
where dd.datum >= dateadd('week', -5, date_trunc('week',current_date)) 
and datum <= current_date
UNION --FOR EU DATA 
select distinct 
	date_trunc ('week', datum)::date as fact_week,
	c.country_name as store_country
from public.dim_dates dd 
	LEFT JOIN (
	select distinct case when s.country_name IN ('Netherlands','Germany','Spain','Austria') THEN 'EU' END AS country_name, min(created_date)::date from ods_production.store s 
		where country_name not in ('United Kingdom','Andorra','United States')
		group by 1) c 
	ON datum >= c.min::date 
where dd.datum >= dateadd('week', -5, date_trunc('week',current_date)) 
and datum <= current_date
order by 1 asc )
,orders as (
select 
distinct
	date_trunc ('week', o.submitted_date)::date as fact_week, 
	o.store_country, 
	count (distinct o.order_id) as submitted_orders
from master.order o
where submitted_date::date is not null
	and date_trunc('week', submitted_date)::Date >= dateadd('week', -6, current_date)
group by 1, 2 
UNION --FOR EU DATA 
SELECT 
distinct
	date_trunc ('week', o.submitted_date)::date as fact_week, 
	CASE WHEN o.store_country NOT IN ('United States') THEN 'EU' END AS store_country, 
	count (distinct o.order_id) as submitted_orders
from master.order o
where submitted_date::date is not null
	and date_trunc('week', submitted_date)::Date >= dateadd('week', -6, current_date)
	AND store_country NOT IN ('United States')
group by 1, 2
) 
, cancelled_subs as 
(	select distinct 
	date_trunc ('week', s.cancellation_date)::date as fact_week,
	s.country_name as store_country,
	count (distinct s.subscription_id) as cancelled_subs
from master.subscription s
where s.cancellation_date is not null
	and date_trunc('week', cancellation_date)::Date >= dateadd('week', -6, current_date)
	and s.cancellation_reason_churn ='customer request'
group by 1, 2
UNION --FOR EU DATA 
	select distinct 
	date_trunc ('week', s.cancellation_date)::date as fact_week,
	CASE WHEN s.country_name NOT IN ('United States') THEN 'EU' END AS store_country,
	count (distinct s.subscription_id) as cancelled_subs
from master.subscription s
where s.cancellation_date is not null
	and date_trunc('week', cancellation_date)::Date >= dateadd('week', -6, current_date)
	and s.cancellation_reason_churn ='customer request'
	AND country_name NOT IN ('United States')
group by 1, 2
)
--INTERCOM
--last close in the last 6 weeks
,time_to_last_close as 
(
select distinct
	date_trunc('week',convert_timezone('Europe/Berlin',ifc.last_close_at::timestamp)::date)::date as fact_week
	,case 
		when ifc.market = 'USA' then 'United States'
		else ifc.market end 
	as store_country
	--count(case when time_to_last_close is not null then id end) as count_time_to_last_close,
	--sum(time_to_last_close) as total_time_to_last_close
	,count(case when time_to_last_close is not null then id end) OVER (PARTITION BY fact_week,store_country) as count_time_to_last_close
	,median( case when time_to_last_close is not null then time_to_last_close/60::decimal(14,2) end)OVER (PARTITION BY fact_week,store_country ) AS median_time_to_last_close
	,sum(time_to_last_close/60::decimal(14,2)) OVER (PARTITION BY fact_week,store_country) AS total_time_to_last_close
	,percentile_cont(0.75) within group ( order by (time_to_last_close/60)::decimal(14,2)) over(PARTITION BY fact_week,store_country) AS percent_75_time_to_last_close
	,percentile_cont(0.95) within group ( order by (time_to_last_close/60)::decimal(14,2)) over(PARTITION BY fact_week,store_country) AS percent_95_time_to_last_close
from ods_data_sensitive.intercom_first_conversation ifc 
where team_participated is not null 
and fact_week >= dateadd('week', -6, current_date) 
UNION --FOR EU DATA 
select distinct
	date_trunc('week',convert_timezone('Europe/Berlin',ifc.last_close_at::timestamp)::date)::date as fact_week
	,case 
		when ifc.market <>'USA' then 'EU' end 
	as store_country
	--count(case when time_to_last_close is not null then id end) as count_time_to_last_close,
	--sum(time_to_last_close) as total_time_to_last_close
	,count(case when time_to_last_close is not null then id end) OVER (PARTITION BY fact_week) as count_time_to_last_close
	,median( case when time_to_last_close is not null then time_to_last_close/60::decimal(14,2) end)OVER (PARTITION BY fact_week ) AS median_time_to_last_close
	,sum(time_to_last_close/60::decimal(14,2)) OVER (PARTITION BY fact_week) AS total_time_to_last_close
	,percentile_cont(0.75) within group ( order by (time_to_last_close/60)::decimal(14,2)) over(PARTITION BY fact_week) AS percent_75_time_to_last_close
	,percentile_cont(0.95) within group ( order by (time_to_last_close/60)::decimal(14,2)) over(PARTITION BY fact_week) AS percent_95_time_to_last_close
from ods_data_sensitive.intercom_first_conversation ifc 
where team_participated is not null 
and fact_week >= dateadd('week', -6, current_date) 
AND market NOT IN ('n/a', 'USA')
--group by 1, 2
)
--first admin reply in the last 6 weeks
,time_to_first_admin_reply as 
(
select distinct
	date_trunc('week',convert_timezone('Europe/Berlin',ifc.first_admin_reply_at::timestamp)::date) as fact_week
	,case 
		when ifc.market = 'USA' then 'United States'
		else ifc.market end 
	as store_country
	--,count(case when time_to_admin_reply is not null then id end) AS count_time_to_first_admin_reply
	--,sum(time_to_admin_reply/60) AS total_time_to_first_admin_reply
	,count(case when time_to_admin_reply is not null then id end) OVER (PARTITION BY fact_week,store_country) as count_time_to_first_admin_reply
	,median( case when time_to_admin_reply is not null then time_to_admin_reply/60::decimal(14,2) end)OVER (PARTITION BY fact_week,store_country ) AS median_time_to_admin_reply
	,sum(time_to_admin_reply/60) OVER (PARTITION BY fact_week,store_country) AS total_time_to_first_admin_reply
	,percentile_cont(0.75) within group ( order by (time_to_admin_reply/60)::decimal(14,2)) over(PARTITION BY fact_week,store_country) AS percent_75_time_to_admin_reply
	,percentile_cont(0.95) within group ( order by (time_to_admin_reply/60)::decimal(14,2)) over(PARTITION BY fact_week,store_country) AS percent_95_time_to_admin_reply
from ods_data_sensitive.intercom_first_conversation ifc 
where team_participated is not null 
and fact_week >= dateadd('week', -6, current_date) 
UNION --FOR EU DATA 
select distinct
	date_trunc('week',convert_timezone('Europe/Berlin',ifc.first_admin_reply_at::timestamp)::date) as fact_week
	,case 
		when ifc.market <>'USA' then 'EU' end 
	as store_country
	--,count(case when time_to_admin_reply is not null then id end) AS count_time_to_first_admin_reply
	--,sum(time_to_admin_reply/60) AS total_time_to_first_admin_reply
	,count(case when time_to_admin_reply is not null then id end) OVER (PARTITION BY fact_week) as count_time_to_first_admin_reply
	,median( case when time_to_admin_reply is not null then time_to_admin_reply/60::decimal(14,2) end)OVER (PARTITION BY fact_week ) AS median_time_to_admin_reply
	,sum(time_to_admin_reply/60) OVER (PARTITION BY fact_week) AS total_time_to_first_admin_reply
	,percentile_cont(0.75) within group ( order by (time_to_admin_reply/60)::decimal(14,2)) over(PARTITION BY fact_week) AS percent_75_time_to_admin_reply
	,percentile_cont(0.95) within group ( order by (time_to_admin_reply/60)::decimal(14,2)) over(PARTITION BY fact_week) AS percent_95_time_to_admin_reply
from ods_data_sensitive.intercom_first_conversation ifc 
where team_participated is not null 
and fact_week >= dateadd('week', -6, current_date) 
AND market NOT IN ('n/a', 'USA')
--GROUP BY 1,2
)
--assigned_conv
, assigned_conv as(
select distinct
	fact_week,
	case 
		when ia.market = 'USA' then 'United States'
		else ia.market end 
	as store_country,
	count (distinct ia.conversation_id) as assigned_conversations
from ods_operations.intercom_assignments ia
where fact_week >= dateadd('week', -6, current_date) 
group by 1, 2
--order by 1, 2 asc 
UNION --FOR EU DATA 
select distinct
	fact_week,
	case 
		when ia.market <>'USA' then 'EU' end 
	as store_country,
	count (distinct ia.conversation_id) as assigned_conversations
from ods_operations.intercom_assignments ia
where fact_week >= dateadd('week', -6, current_date) 
AND market NOT IN ('n/a', 'USA')
group by 1, 2
--order by 1, 2 asc 
)
--rating final
,rating_final as(
select
	distinct 
	rating_week as fact_week,
	(case 
		when market = 'USA' then 'United States'
		else market end) 
	as store_country,
	count (distinct r.conversation_id) as all_conversations_rated,
	count (distinct case when r.conversation_rating = 5 or r.conversation_rating = 4 then r.conversation_id else null end) as good_rating
	from ods_operations.conversation_rating_intercom r 
	where r.desc_idx = 1
	and fact_week >= dateadd('week', -6, current_date)
	group by 1, 2
	UNION --FOR EU DATA 
	select
	distinct 
	rating_week as fact_week,
	(case 
		when market <> 'USA' then 'EU'
		else market end) 
	as store_country,
	count (distinct r.conversation_id) as all_conversations_rated,
	count (distinct case when r.conversation_rating = 5 or r.conversation_rating = 4 then r.conversation_id else null end) as good_rating
	from ods_operations.conversation_rating_intercom r 
	where r.desc_idx = 1
	and fact_week >= dateadd('week', -6, current_date)
	AND market NOT IN ('n/a','USA')
	group by 1, 2	
)
select 
	distinct 
	f.fact_week,
	f.store_country,
	coalesce(sum(o.submitted_orders),0) as submitted_orders,
	coalesce(sum(c.cancelled_subs),0) as cancelled_subs,
	coalesce(sum(assigned_conversations),0) as assigned_conversations,
	sum(count_time_to_first_admin_reply)::decimal(14,2) as count_time_to_first_admin_reply,
	sum(total_time_to_first_admin_reply)::decimal(14,2) as total_time_to_first_admin_reply,
	sum(median_time_to_admin_reply)::decimal(14,2) AS median_time_to_admin_reply,
	sum(percent_75_time_to_admin_reply) AS percent_75_time_to_admin_reply,
	sum(percent_95_time_to_admin_reply) AS percent_95_time_to_admin_reply,
	sum(count_time_to_last_close) as count_time_to_last_close,
	sum(total_time_to_last_close) as total_time_to_last_close,
	sum(median_time_to_last_close)::decimal(14,2) AS median_time_to_last_close,
	sum(percent_75_time_to_last_close) AS percent_75_time_to_last_close,
	sum(percent_95_time_to_last_close) AS percent_95_time_to_last_close,
	sum(all_conversations_rated) as all_conversations_rated,
	sum(good_rating) as good_rating
from fact_ f 
left join orders o 
	on f.fact_week = o.fact_week and f.store_country = o.store_country
left join cancelled_subs c 
	on f.fact_week = c.fact_week and f.store_country = c.store_country
left join assigned_conv ci
	on f.fact_week = ci.fact_week and f.store_country = ci.store_country
left join time_to_last_close lc 
	on f.fact_week = lc.fact_week and f.store_country = lc.store_country 
left join time_to_first_admin_reply fa 
	on f.fact_week = fa.fact_week and f.store_country = fa.store_country 
left join rating_final r
	on f.fact_week = r.fact_week and f.store_country = r.store_country
group by 1, 2
order by 1, 2 desc 
WITH NO SCHEMA BINDING;