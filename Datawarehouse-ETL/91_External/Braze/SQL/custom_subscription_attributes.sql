-- STEP 1: Staging attributes
drop table if exists dm_marketing.stg_braze_subscriptions;

create table dm_marketing.stg_braze_subscriptions as
with a as
(select
	distinct s.customer_id,
    c.updated_at,
	c.active_subscriptions,
	subscription_id,
	s.subscription_sf_id,
	s.minimum_term_months,
	s.paid_subscriptions,
	s.product_name,
	s.start_date,
	s.minimum_cancellation_date::Date,
	(s.minimum_cancellation_date::Date - CURRENT_DATE::Date) as days_until_cancellation,
	(s.rental_period::text + '_' + s.rank_subscriptions::text) as rental_rank
from master.subscription s
	left join master.customer c
	on s.customer_id = c.customer_id
where s.status ='ACTIVE'
	order by s.rank_subscriptions desc ),
	min_days as
(SELECT
	distinct customer_id,
	case when min(days_until_cancellation) < 0 then 'yes' else 'no' end as customers_extended_subs,
	min(days_until_cancellation) as closest_day_count
from a
	group by 1)
SELECT
	DISTINCT a.customer_id as external_id,
--	customers_extended_subs,
--	closest_day_count,
	listagg(product_name, ', ') as almost_ending_subscription_names,
	listagg(subscription_id,', ') as almost_ending_subscription_ids,
	listagg(rental_rank,', ') as almost_ending_subscription_periods,
	count(subscription_id) as same_day_ending_subs_count,
	min(minimum_cancellation_date) as closest_minimum_cancellation_date,
	min(active_subscriptions) as active_subscriptions
from a
	left join min_days m
	on a.customer_id = m.customer_id
where customers_extended_subs ='no' --this is the logic that excludes customers that continued renting past their cancellation date
and a.minimum_cancellation_date::date between CURRENT_DATE  and CURRENT_DATE + 30
and (a.updated_at) > (select updated_at from trans_dev.braze_date_cntrl2)
group by 1 order by closest_minimum_cancellation_date;
    
 

-- Update date control table
update trans_dev.braze_date_cntrl2 set updated_at = (select max(c.updated_at) from master.customer c);



-- STEP 2: Export attributes
drop table if exists dm_marketing.braze_subscription_export;

create table dm_marketing.braze_subscription_export as 
select stg.* from dm_marketing.stg_braze_subscriptions stg
minus
select hist.* from dm_marketing.braze_subscription_export_history hist;




--STEP 3: Merge delta to historical
-- Deleting previous records

delete from dm_marketing.braze_subscription_export_history
using dm_marketing.stg_braze_subscriptions
where dm_marketing.braze_subscription_export_history."external_id" = dm_marketing.stg_braze_subscriptions."external_id";

-- Insert new records

insert into dm_marketing.braze_subscription_export_history
select * from dm_marketing.stg_braze_subscriptions;



