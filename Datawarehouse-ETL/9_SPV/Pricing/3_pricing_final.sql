drop table if exists dm_commercial.pricing_final;
create table dm_commercial.pricing_final as 
with a as(
select
	DISTINCT
  trim(p.product_sku) as product_sku,
  avg(price::double precision) as price
from stg_external_apis.mm_price_data m
INNER join ods_production.variant v on v.ean::int8=m.ean
left join ods_production.product p on p.product_id=v.product_id
where m.valid_from::date>DATEADD(day,-14, GETDATE()) 
and v.ean SIMILAR to '[0-9]*'
group by 1
),
 b as
(
select * from 
(select 
	product_sku,
	date_trunc('week',created_at) as created_at,
	avg(initial_price) as initial_price ,
	row_number() over(partition by product_sku order by date_trunc('week',created_at) desc) as rn
from master.asset group by 1,2 )
where rn = 1   
)
select 
	reporting_date,
	m.product_sku,
	case when asset_condition='Neu' then final_market_price end as "new_price",
	case when asset_condition='Neu' then months_since_last_price end as "months_since_last_new_price",
    case when asset_condition='Wie neu' then final_market_price end as "wie_neu_price",
	case when asset_condition='Wie neu' then months_since_last_price end as "months_since_last_wieneu_price",
    case when asset_condition='Sehr gut' then final_market_price end as "sehr_gut_price",
	case when asset_condition='Wie Sehr gut' then months_since_last_price end as "months_since_last_sehr_gut_price",
	(a.price/case when asset_condition='Neu' then final_market_price end )-1 as "new_vs_mm",
	a.price as mm_price,
	b.initial_price as pp_price,
	(current_date-b.created_at::date)/30 as months_since_last_pp_price
	from dm_commercial.pricing_spv_used_asset_price_master m
	left join a on a.product_sku=m.product_sku
	left join b on b.product_sku=m.product_sku