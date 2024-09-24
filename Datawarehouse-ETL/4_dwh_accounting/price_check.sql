drop table if exists dwh.price_check;
create table dwh.price_check as 
with a as(
	select 
	mm.fact_day as fact_day, 
	mm.ean as ean, 
	mm.price as price, 
	'Media Markt' as table
from dwh.media_markt_prices mm
union all
	select 
	pp.fact_day as fact_day,
	pp.ean as ean,
	pp.price as price, 
	'Saturn' as table
from dwh.saturn_prices pp)
select *
from a;
