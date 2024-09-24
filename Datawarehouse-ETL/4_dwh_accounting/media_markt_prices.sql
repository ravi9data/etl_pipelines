-- Media Markt prices with respect to fact day for accounting

drop table if exists dwh.media_markt_prices;
create table dwh.media_markt_prices as 
with a as (
	select *,
	rank() over (partition by ean,valid_from::date order by valid_from DESC) as day_rank from stg_external_apis.mm_price_data
				order by valid_from DESC),
		b as (
		select distinct * from a where day_rank = 1 
		and valid_to is not null
		)
		,fact_days as 
		(SELECT
	DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
		)
		select 
			* from b
		left join fact_days fd on (fd.fact_day >= b.valid_from::date ) and (fd.fact_day < valid_to::date ) 
					order by fact_day ASC;


-- Saturn prices with respect to fact days for accounting 

drop table if exists dwh.saturn_prices;
create table dwh.saturn_prices as 
with a as (
	select *,
	rank() over (partition by ean,valid_from::date order by valid_from DESC) as day_rank from stg_external_apis.saturn_price_data
				order by valid_from DESC),
		b as (
		select distinct * from a where day_rank = 1 
		and valid_to is not null
		)
		,fact_days as 
		(SELECT
	DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
		)
		select 
			* from b
		left join fact_days fd on (fd.fact_day >= b.valid_from::date ) and (fd.fact_day < valid_to::date ) 
					order by fact_day ASC;