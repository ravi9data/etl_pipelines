drop table if exists ods_production.customer_rfm_segmentation;
create table ods_production.customer_rfm_segmentation as 
with a as (
select s.customer_id, max(cf.max_cashflow_date) as max_cashflow_date, sum(total_cashflow) as total_cashflow
from ods_production.subscription s 
left join ods_production.subscription_cashflow cf 
 on s.subscription_id=cf.subscription_id
group by 1)
,prep as (
select c.customer_id, least(cs.subscriptions,10) as subscriptions, a.max_cashflow_date, 
greatest(coalesce((current_date::date-a.max_cashflow_date::date)::int,99999),0) as days_since_last_cashflow,
greatest(a.total_cashflow,0) as total_cashflow, 
PERCENT_RANK() OVER (order by cs.subscriptions asc) as frequency_rank,
PERCENT_RANK() OVER (order by greatest(coalesce((current_date::date-a.max_cashflow_date::date)::int,99999),0) desc) as recency_rank,
PERCENT_RANK() OVER (order by greatest(a.total_cashflow,0) asc) as monetary_rank
from ods_production.customer c 
left join ods_production.customer_subscription_details cs --check
  on cs.customer_id = c.customer_id
left join a on a.customer_id=c.customer_id
where cs.subscriptions>=1)
,prep2 as(
select 
 p.*,
 case 
  when frequency_rank <=0.25 then 4
  when frequency_rank >0.25 and frequency_rank <=0.5 then 3
  when frequency_rank >0.5 and frequency_rank <=0.75 then 2
  when frequency_rank >0.75 and frequency_rank <=1 then 1
 end as frequency_score,
 case 
  when recency_rank <=0.25 then 4
  when recency_rank >0.25 and recency_rank <=0.5 then 3
  when recency_rank >0.5 and recency_rank <=0.75 then 2
  when recency_rank >0.75 and recency_rank <=1 then 1
 end as recency_score,
 case 
  when monetary_rank <=0.25 then 4 
  when monetary_rank >0.25 and monetary_rank <=0.5 then 3
  when monetary_rank >0.5 and monetary_rank <=0.75 then 2
  when monetary_rank >0.75 and monetary_rank <=1 then 1
 end as monetary_score
from prep p)
select 
 *,
 (recency_score*100+frequency_score*10+monetary_score) as rfm_score,
 case 
  when (recency_score*100+frequency_score*10+monetary_score) = 111 
   then 'Best'
  when frequency_score=1 
   then 'Loyal'
  when monetary_score=1 
   then 'Big_Spenders'
  when (recency_score*100+frequency_score*10+monetary_score) = 311
   then 'Almost Lost'
  when (recency_score*100+frequency_score*10+monetary_score) = 411
   then 'Lost_Customers'
  when (recency_score*100+frequency_score*10+monetary_score) = 444
   then 'Lost_Cheap'
  else 'Unclassified'
   end as rfm_segment
from prep2 p 
;

GRANT SELECT ON ods_production.customer_rfm_segmentation TO tableau;
