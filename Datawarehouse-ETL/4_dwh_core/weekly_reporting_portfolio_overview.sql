--get only relevant data
drop table if exists product_reporting_base;
create temp table product_reporting_base as 
select 
	  fact_day ,
	  account_name,
	  category_name,
	  store_country,
	  store_name,
	  store_short,	
	  case when store_name like '%B2B%' then 'B2B'
	  	   when store_short  like '%Partners%' then 'Retail'
	  	   when account_name in ('Grover - Germany' , 'Grover - Austria' ,'Grover - Netherlands' , 'Grover - Spain','Grover - United States')
	  	   then 'B2C' end customer_type,
	  acquired_committed_subscription_value,
	  acquired_subscription_value,
	  acquired_subscriptions,
	  active_subscription_value,
	  asset_investment,
	  avg_price,
	  cancelled_subscription_value,
	  completed_orders ,
	  inventory_debt_collection ,
	  inventory_in_stock ,
	  inventory_irreparable ,
	  inventory_lost ,
	  inventory_on_rent ,
	  inventory_others ,
	  inventory_refurbishment ,
	  inventory_repair ,
	  inventory_selling ,
	  inventory_sold ,
	  inventory_sold_to_customers ,
	  inventory_sold_to_3rdparty ,
	  inventory_inbound ,
	  inventory_inbound_unallocable ,
	  inventory_transition ,
	  inventory_writtenoff_ops ,
	  inventory_writtenoff_dc ,
	  inventory_writtenoff ,
	  pageview_unique_sessions ,
	  paid_orders ,
	  paid_short_term_orders 
from dwh.product_reporting 
where fact_day >= dateadd('month', -18, date_trunc('month', current_date)) 
	  and account_name NOT IN ('Grover - USA old' , 'Grover - UK')
	  and (category_name is not null or category_name not in ('POS'))
	  and store_country  in ('Germany', 'Austria', 'Netherlands', 'Spain', 'United States');
	  
-- investment capital, will be used in both reports
-- only calculate first day of the week and the month
drop table if exists investment_capital_temp;
create temp table investment_capital_temp as	
with fact_days as (
	select datum as fact_day 
	from public.dim_dates 
	where (week_day_number = 1
	   or day_is_first_of_month = 1)
)
select 
	f.fact_day,
	s.category_name,
	sum(s.initial_price) as investment_capital
from fact_days f , ods_production.asset s 
where f.fact_day >= s.purchased_date 
	and (f.fact_day<=s.sold_date OR s.sold_date is null) 
	and s.asset_status_grouped <> 'NEVER PURCHASED'
group by 1,2;
	

--MONTHLY
--first get last day of the month snapshots
--then calculate cumulative ones
drop table if exists dm_weekly_monthly.category_performance_monthly;
create table dm_weekly_monthly.category_performance_monthly as 	 
with
eom as ( 
select 
	date_trunc('month', fact_day) fact_day,
	category_name ,
	store_country ,
	customer_type ,
	sum(active_subscription_value) as active_subscription_value ,
	sum(asset_investment) as asset_investment,
	sum(inventory_debt_collection) as inventory_debt_collection,
	sum(inventory_in_stock) as inventory_in_stock,
	sum(inventory_irreparable) as inventory_irreparable,
	sum(inventory_lost) as inventory_lost,
	sum(inventory_on_rent) as inventory_on_rent,
	sum(inventory_others) as inventory_others,
	sum(inventory_refurbishment) as inventory_refurbishment,
	sum(inventory_repair) as inventory_repair,
	sum(inventory_selling) as inventory_selling,
	sum(inventory_sold) as inventory_sold,
	sum(inventory_sold_to_customers) as inventory_sold_to_customers,
	sum(inventory_sold_to_3rdparty) as inventory_sold_to_3rdparty,
	sum(inventory_inbound) as inventory_inbound,
	sum(inventory_inbound_unallocable) as inventory_inbound_unallocable,
	sum(inventory_transition) as inventory_transition,
	sum(inventory_writtenoff_ops) as inventory_writtenoff_ops,
	sum(inventory_writtenoff_dc) as inventory_writtenoff_dc,
	sum(inventory_writtenoff) as inventory_writtenoff
from product_reporting_base 
where date_part('day', dateadd('day', 1, fact_day)) = 1
group by 1,2,3,4)
, 
cumulative as (
select 
	date_trunc('month', fact_day) fact_day,
	category_name ,
	store_country ,
	customer_type ,
	sum(completed_orders) as completed_orders,
	sum(pageview_unique_sessions) as pageview_unique_sessions,
	sum(acquired_subscription_value) as acquired_subscription_value,
	sum(cancelled_subscription_value) as cancelled_subscription_value,
	sum(acquired_subscriptions) as acquired_subscriptions,
	sum(paid_orders) as paid_orders, 
	sum(paid_short_term_orders) as paid_short_term_orders
from product_reporting_base
group by 1,2,3,4)
select 
	e.*, 
	c.completed_orders,
	c.pageview_unique_sessions ,
	c.cancelled_subscription_value,
	c.acquired_subscriptions,
	c.acquired_subscription_value,
	c.paid_orders,
	c.paid_short_term_orders,
	i.investment_capital * --we do not have granularity here, so let's distribute capital based on asv
	case when e.active_subscription_value = 0 then 0 
		 else e.active_subscription_value::float / 
		 		sum(e.active_subscription_value) over (partition by e.fact_day, e.category_name)::float
		 end as investment_capital
from eom e 
left join cumulative c 
on e.fact_day = c.fact_day and e.category_name = c.category_name and e.store_country = c.store_country and e.customer_type = c.customer_type
left join investment_capital_temp i 
on e.fact_day = i.fact_day and e.category_name = i.category_name;
	
GRANT SELECT ON dm_weekly_monthly.category_performance_monthly TO tableau;
	  

--- WEEKLY
--first get last day of the week snapshot
--then calculate cumulative ones
drop table if exists dm_weekly_monthly.category_performance_weekly;
create table dm_weekly_monthly.category_performance_weekly as
with
eow as ( 
select 
	date_trunc('week', fact_day) fact_day,
	category_name ,
	store_country ,
	customer_type ,
	sum(active_subscription_value) as active_subscription_value ,
	sum(asset_investment) as asset_investment,
	sum(inventory_debt_collection) as inventory_debt_collection,
	sum(inventory_in_stock) as inventory_in_stock,
	sum(inventory_irreparable) as inventory_irreparable,
	sum(inventory_lost) as inventory_lost,
	sum(inventory_on_rent) as inventory_on_rent,
	sum(inventory_others) as inventory_others,
	sum(inventory_refurbishment) as inventory_refurbishment,
	sum(inventory_repair) as inventory_repair,
	sum(inventory_selling) as inventory_selling,
	sum(inventory_sold) as inventory_sold,
	sum(inventory_sold_to_customers) as inventory_sold_to_customers,
	sum(inventory_sold_to_3rdparty) as inventory_sold_to_3rdparty,
	sum(inventory_inbound) as inventory_inbound,
	sum(inventory_inbound_unallocable) as inventory_inbound_unallocable,
	sum(inventory_transition) as inventory_transition,
	sum(inventory_writtenoff_ops) as inventory_writtenoff_ops,
	sum(inventory_writtenoff_dc) as inventory_writtenoff_dc,
	sum(inventory_writtenoff) as inventory_writtenoff
from product_reporting_base 
where date_part('dayofweek', dateadd('day', 1, fact_day)) = 1
  and fact_day >= dateadd('week', -8, date_trunc('week', current_date)) 
group by 1,2,3,4)
, 
cumulative as (
select 
	date_trunc('week', fact_day) fact_day,
	category_name ,
	store_country ,
	customer_type ,
	sum(completed_orders) as completed_orders,
	sum(pageview_unique_sessions) as pageview_unique_sessions,
	sum(acquired_subscription_value) as acquired_subscription_value,
	sum(cancelled_subscription_value) as cancelled_subscription_value,
	sum(acquired_subscriptions) as acquired_subscriptions,
	sum(paid_orders) as paid_orders, 
	sum(paid_short_term_orders) as paid_short_term_orders
from product_reporting_base
where fact_day >= dateadd('week', -8, date_trunc('week', current_date)) 
group by 1,2,3,4),
--
-- Targets
--
category_targets as (
	SELECT
		date_trunc('week', "to_date") AS fact_day,
		case when store_label = 'DE' then 'Germany'
			 when store_label = 'AT' then 'Austria'
			 when store_label = 'NL' then 'Netherlands'
			 when store_label = 'ES' then 'Spain'
			 when store_label = 'US' then 'United States'
		end	AS country,
		channel_type AS channel_type,
		categories AS category_name,
		sum(amount) AS active_subscription_value_target
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE measures = 'ASV'
	and "to_date" <= current_date
	and "to_date" >= dateadd('week', -8, date_trunc('week', current_date)) 
	and date_part('dayofweek', dateadd('day', 1, "to_date")) = 1
	GROUP BY 1,2,3,4
)
select 
	w.*, 
	c.completed_orders,
	c.pageview_unique_sessions ,
	c.cancelled_subscription_value,
	c.acquired_subscriptions,
	c.acquired_subscription_value,
	c.paid_orders,
	c.paid_short_term_orders,
	i.investment_capital *
	case when w.active_subscription_value = 0 then 0 
		 else w.active_subscription_value::float / 
		 		sum(w.active_subscription_value) over (partition by w.fact_day, w.category_name)::float
		 end as investment_capital,
	t.active_subscription_value_target
from eow w 
left join cumulative c 
on w.fact_day = c.fact_day and w.category_name = c.category_name and w.store_country = c.store_country and w.customer_type = c.customer_type
left join investment_capital_temp i 
on w.fact_day = i.fact_day and w.category_name = i.category_name
left join category_targets t 
on w.fact_day = t.fact_day and w.category_name = t.category_name and w.store_country = t.country and w.customer_type = t.channel_type ;



GRANT SELECT ON dm_weekly_monthly.category_performance_weekly TO tableau;
