drop table if exists dwh.active_customer;
create table dwh.active_customer as 
WITH FACT_DAYS AS (
SELECT
DISTINCT DATUM AS FACT_DAY
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
select 
 f.fact_day,
 /*##################################
 ACTIVE CUSTOMERS 
 ##################################*/
(select count(distinct s.customer_id) from ods_production.subscription as s
where f.fact_day::date >= s.start_date::date 
 and f.fact_day::date < coalesce(s.cancellation_date::date, f.fact_day::date+1)) as active_customers,
 /*##################################
 RECURRING OR ACTIVE CUSTOMERS 
 ##################################*/
 (select count(distinct s.customer_id) from ods_production.subscription as s
where (f.fact_day::date >= s.start_date::date and
f.fact_day::date < coalesce(s.cancellation_date::date, f.fact_day::date+1)) or (rank_subscriptions=2 and f.fact_day::date >= s.start_date::date)) recurring_or_active_customers,
 /*##################################
 CUMULATIVE CUSTOMERS 
 ##################################*/
(select count(distinct s.customer_id) from ods_production.subscription as s
where f.fact_day::date >= s.start_date::date ) as cumulative_customers,
/*##################################
 CUSTOMERS WITH PAYMENT IN LAST 12 MONTHS
 ##################################*/
(select count(distinct s.customer_id) from ods_production.payment_subscription as s
where s.paid_date::date >= fact_day-12*30 and s.paid_date <= fact_day ) as customers_paid_last_12m,
/*##################################
 ACTIVE CUSTOMERS + Recent Churn (12 months cutoff)
 ##################################*/
(select count(distinct s.customer_id) from ods_production.subscription as s
where (f.fact_day::date >= s.start_date::date 
 and f.fact_day::date < coalesce(s.cancellation_date::date+30*12, f.fact_day::date+1))) as customers_active_12m_churn,
 /*##################################
 ACTIVE CUSTOMERS + Recent Churn (6 months cutoff)
 ##################################*/
(select count(distinct s.customer_id) from ods_production.subscription as s
where (f.fact_day::date >= s.start_date::date 
 and f.fact_day::date < coalesce(s.cancellation_date::date+30*6, f.fact_day::date+1))) as customers_active_6m_churn 
from fact_days as f
group by f.fact_day
order by 1 desc;

GRANT SELECT ON dwh.active_customer TO tableau;
