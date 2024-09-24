--asset_valuation_quality_meas_tool_report_source_dis_asset_condition_dis
drop view if exists dm_finance.v_asset_valuation_quality_meas_tool_report_source_dis_asset_condition_dis;
create view dm_finance.v_asset_valuation_quality_meas_tool_report_source_dis_asset_condition_dis as
with dates as (
	select date_trunc('month',datum)::date as month
	from public.dim_dates dd where datum<current_date-1)
,prod as(
	select distinct date_trunc('month',date)::date as hist_date, 
	count(distinct product_sku) as prod_count_hist
	from master.Variant_historical ah 
group by 1)
, spv as (
	select src,
	date_trunc('month',extract_date)::date as extract_date,
	count(distinct product_sku) as prod_count_moz_src
	from ods_spv_historical.union_sources us where price is not null
	group by 1,2)
, ac as (
	select asset_condition,
	date_trunc('month',extract_date)::date as extract_date,
	count(distinct product_sku) as prod_count_moz_ac
	from ods_spv_historical.union_sources us where price is not null
	group by 1,2
)
, prep as (select d.month,
s.src,
asset_condition,
p.prod_count_hist,
s.prod_count_moz_src,
a.prod_count_moz_ac
from dates d
left join prod p
on d.month=p.hist_date
left join spv s 
on d.month=s.extract_date
left join ac a 
on d.month=a.extract_date)
select distinct month, src,asset_condition,prod_count_hist,prod_count_moz_src,prod_count_moz_ac from prep
with no schema binding;