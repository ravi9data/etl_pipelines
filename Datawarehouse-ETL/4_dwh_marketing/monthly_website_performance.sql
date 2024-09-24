drop table if exists dwh.monthly_website_performance;
CREATE TABLE dwh.monthly_website_performance AS

		with web_vitals as (
	select DATE_TRUNC('month',collector_tstamp) as fact_date,
	avg(lcp) as avg_lcp,
	avg(ttfb) as avg_ttfb,
	avg(fid) as avg_fid,
	avg(cls) as avg_cls
	from	web.web_vitals
	group by 1 
	order by 1 desc)
	,fact_date as(
	select distinct DATE_TRUNC('month',datum::date) as fact_date
	from public.dim_dates dd 
	where datum < current_date and datum > '2019-05-01'
	order by 1 desc 
	)
	, page_views as (
	select DATE_TRUNC('month',page_view_date::date) as fact_date,
	count(page_view_id) as  page_views
	from traffic.page_views
	group by 1 
	order by 1 desc )
	select fd.fact_date::date, 
	avg_lcp,
	avg_ttfb,
	avg_fid,
	avg_cls,
	pv.page_views
	from fact_date fd
	left join web_vitals wv on wv.fact_date = fd.fact_date
	left join page_views pv on pv.fact_date = fd.fact_date
	order by 1 desc;