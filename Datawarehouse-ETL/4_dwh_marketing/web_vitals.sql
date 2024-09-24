
drop table if exists dwh.website_performance;
CREATE TABLE dwh.website_performance AS
		with web_vitals as (
	select DATE_TRUNC('week',collector_tstamp) as fact_date,
	avg(lcp) as avg_lcp,
	avg(ttfb) as avg_ttfb,
	avg(fid) as avg_fid,
	avg(cls) as avg_cls
	from	web.web_vitals
	where collector_tstamp > CURRENT_DATE-90 --only to consider last 3 months data
	group by 1 
	order by 1 desc)
	, page_views as (
	select DATE_TRUNC('week',page_view_date::date) as fact_date,
	count(page_view_id) as  page_views
	from traffic.page_views
	where page_view_date > CURRENT_DATE-90
	group by 1 
	order by 1 desc )
	select wv.*,pv.page_views
	from web_vitals wv 
	left join page_views pv on wv.fact_date = pv.fact_date;