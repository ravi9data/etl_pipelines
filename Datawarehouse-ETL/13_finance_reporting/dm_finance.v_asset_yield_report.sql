--asset_yield_report
DROP VIEW IF EXISTS dm_finance.v_asset_yield_report;
CREATE VIEW dm_finance.v_asset_yield_report AS 
WITH query1 AS (
with fact_days as (
select DISTINCT datum as fact_day from public.dim_dates
where datum<=CURRENT_DATE)
select 
	f.fact_day,
	(select sum(s.subscription_value) from ods_production.subscription as s
		where f.fact_day between s.start_date and coalesce(s.cancellation_date, f.fact_day)) as total_volume,
	(select sum(s.subscription_value) from ods_production.subscription as s
		where f.fact_day between s.start_date and coalesce(s.cancellation_date, f.fact_day) AND
		not exists (select 1 from ods_production.payment_subscription as sp where s.subscription_id = sp.subscription_id
			and sp.status in ('FAILED','FAILED FULLY','NOT PROCESSED'))) as paid_volume
from fact_days as f
group by f.fact_day
)
,query2 AS (
select 
	f.datum::DATE as fact_day,
	count(DISTINCT s.paid_date::DATE) as dates,
	sum(s.amount_paid) as cash_flow
from public.dim_dates f, ods_production.payment_all as s
where s.paid_date::DATE<=f.datum::DATE	and s.paid_date::DATE>f.datum::DATE-30
--where datediff(day,f.fact_day::date,s.paid_date::date)<30 and datediff(day,f.fact_day::date,s.paid_date::date)>=0
	and f.datum::DATE<=CURRENT_DATE
	group by 1
)
, query3 AS (
	with ab as (		
--select DISTINCT subcategory_name,categorylevelii, count(*) from (
select 
		s.subscription_id,
		s.variant_sku,
		p.subcategory_name,
		g.categorylevelii,
		g.opscost::int,
		s.cancellation_date::date as c_date
	from ods_production.subscription s
	left join ods_production.variant v on v.variant_sku=s.variant_sku
	left join ods_production.product p on p.product_id=v.product_id
	left join public.ops_cost g on g.categorylevelii=p.subcategory_name
	where status='CANCELLED'
)
select 
	f.datum, 
	sum(s.opscost) opscost 
from public.dim_dates f, ab as s
		where  s.c_date::DATE<=f.datum::DATE	and s.c_date::DATE>f.datum::DATE-30
	and f.datum::DATE<=CURRENT_DATE
group by 1
)
,query4 AS (
WITH fact_days AS (
select DISTINCT datum as fact_day from public.dim_dates
where datum<=CURRENT_DATE)
select 
	fact_day,
	'Excluding Sold Assets' as status,
	sum(s.initial_price) as investment_capital
from fact_days f, ods_production.asset as s
	where f.fact_day>=s.purchased_date and s.asset_status_grouped<>'NEVER PURCHASED' and (f.fact_day<=s.sold_date or s.sold_date is NULL) 	
	group by 1,2
)
SELECT 
  COALESCE(q1.fact_day, q3.datum) AS fact_day
  ,q1.total_volume
  ,q1.paid_volume
  ,q2.dates
  ,q2.cash_flow
  ,q3.opscost
  ,q4.status
  ,q4.investment_capital
FROM query1 q1
  LEFT JOIN query2 q2
    ON q1.fact_day = q2.fact_day
  LEFT JOIN query4 q4
    ON q1.fact_day = q4.fact_day
  FULL OUTER JOIN query3 q3 
    ON q1.fact_day = q3.datum
WITH NO SCHEMA BINDING
;