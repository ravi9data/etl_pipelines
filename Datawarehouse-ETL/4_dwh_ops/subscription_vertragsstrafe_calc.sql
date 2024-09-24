drop table if exists dwh.subscription_vertragsstrafe_calc;
create table dwh.subscription_vertragsstrafe_calc as 
with historical_prices as (
select
 date_trunc('week',o.created_date)::date as created_at,
 v.product_id,
 s.store_short,
 max(case when plan_duration = 1 then price end)  as price_1m,
 max(case when plan_duration = 3 then price end)  as price_3m,
 max(case when plan_duration = 6 then price end)  as price_6m,
 max(case when plan_duration = 12 then price end)  as price_12m
from ods_production.order_item oi
 left join ods_production.variant v 
  on v.variant_id=oi.variant_id
left join ods_production."order" o 
 on o.order_id=oi.order_id
left join ods_production.store s on s.id=o.store_id
--where product_id ='7949'
-- and store_short = 'Grover'
and store_id is not null
group by 1,2,3
/*select
 date_trunc('week',start_date)::date as created_at,
 v.product_id,
 s.store_short,
 max(case when minimum_term_months = 1 then subscription_value end)  as price_1m,
 max(case when minimum_term_months = 3 then subscription_value end)  as price_3m,
 max(case when minimum_term_months = 6 then subscription_value end)  as price_6m,
 max(case when minimum_term_months = 12 then subscription_value end)  as price_12m
from ods_production.subscription s
 left join ods_production.variant v 
  on v.variant_sku=s.variant_sku
--where product_id ='7949'
-- and store_short = 'Grover'
and store_id is not null
group by 1,2,3*/)
, sums as(
select 
 created_at,
 product_id,
 store_short,
 coalesce(price_1m,last_value(price_1m ignore nulls) over (partition by product_id, store_short order by created_at
 rows between unbounded preceding and unbounded following)) as price_1m_hisotrical,
 coalesce(price_3m,last_value(price_3m ignore nulls) over (partition by product_id, store_short order by created_at
 rows between unbounded preceding and unbounded following)) as price_3m_hisotrical,
  coalesce(price_6m,last_value(price_6m ignore nulls) over (partition by product_id, store_short order by created_at
 rows between unbounded preceding and unbounded following)) as price_6m_hisotrical,
   coalesce(price_12m,last_value(price_12m ignore nulls) over (partition by product_id, store_short order by created_at
 rows between unbounded preceding and unbounded following)) as price_12m_hisotrical
from historical_prices)
 /*where store_short = 'Grover'
  and product_id = '8441';
*/
--  select * from ods_production.product where product_name = 'Laptop Apple MacBook Air i5/16GB/128GB/UHD615 (Late 2018)';
,a as (select store_id, product_id, 
cast (max(case when minimum_term_months = 1 then rental_plan_price end)as double precision)  as price_1m,
cast (max(case when minimum_term_months = 3 then rental_plan_price end)as double precision)  as price_3m,
cast (max(case when minimum_term_months = 6 then rental_plan_price end)as double precision)  as price_6m,
cast (max(case when minimum_term_months = 12 then rental_plan_price end)as double precision)  as price_12m
from s3_spectrum_rds_dwh_api_production.rental_plans
where active='True'
group by 1,2)
select
 p.product_id,
 ss.store_id,
 ss.store_short,
 s.customer_id,
 s.start_date,
 s.subscription_id, 
 s.subscription_sf_id,
 p.product_name, 
 p.product_sku,
 s.minimum_term_months,
 s.paid_subscriptions,
  case when paid_subscriptions <= 2 then price_1m 
  when paid_subscriptions <= 5 then price_3m
  when paid_subscriptions <= 11 then price_6m 
  else price_12m end as price_to_take,
   price_1m,price_3m,price_6m,price_12m,
 s.subscription_value,
 ((case when paid_subscriptions <= 2 then price_1m 
  when paid_subscriptions <= 5 then price_3m
  when paid_subscriptions <= 11 then price_6m 
  else price_12m end) - s.subscription_value)*paid_subscriptions +
  (case when paid_subscriptions <= 2 then price_1m 
  when paid_subscriptions <= 5 then price_3m
  when paid_subscriptions <= 11 then price_6m 
  else price_12m end) as vertragsstrafe,
  price_1m_hisotrical,price_3m_hisotrical,price_6m_hisotrical,price_12m_hisotrical,
   ((case when paid_subscriptions <= 2 then price_1m_hisotrical 
  when paid_subscriptions <= 5 then price_3m_hisotrical
  when paid_subscriptions <= 11 then price_6m_hisotrical
  else price_12m_hisotrical end) - s.subscription_value)*paid_subscriptions +
  (case when paid_subscriptions <= 2 then price_1m_hisotrical
  when paid_subscriptions <= 5 then price_3m_hisotrical
  when paid_subscriptions <= 11 then price_6m_hisotrical
  else price_12m_hisotrical end) as vertragsstrafe_historical
from master.subscription s
left join ods_production.subscription ss
 on s.subscription_id = ss.subscription_id
left join ods_production.product p 
 on p.product_sku=s.product_sku
left join a 
 on a.store_id=ss.store_id
 and a.product_id=p.product_id
left join sums 
 on sums.created_at=date_trunc('week',s.order_created_date)
 and sums.product_id=p.product_id
 and sums.store_short=ss.store_short
--where s.customer_id = '191913'
;

GRANT SELECT ON dwh.subscription_vertragsstrafe_calc TO tableau;
