--asset_valuation_quality_meas_tool_report
drop view if exists dm_finance.v_asset_valuation_quality_meas_tool_report;
create view dm_finance.v_asset_valuation_quality_meas_tool_report as
with dates as (
	select datum::date as month
	from public.dim_dates dd where datum<current_date-1)
,prod as(
	select distinct date as hist_date, 
	count(distinct product_sku) as prod_count_hist
	from master.variant_historical ah 
group by 1)
, spv as (
	select src,
	extract_date,
        case when src is null then 0 else 1 end as scraped,
	count(distinct product_sku) as prod_count_moz
	from ods_spv_historical.union_sources us where price is not null
	group by 1,2,3)
select d.month,
s.src,
s.scraped,
p.prod_count_hist,
s.prod_count_moz
from dates d
left join prod p
on d.month=p.hist_date
left join spv s 
on d.month=s.extract_date
with no schema binding;