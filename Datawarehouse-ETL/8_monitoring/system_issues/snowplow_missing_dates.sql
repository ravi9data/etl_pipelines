drop table if exists monitoring.snowplow_missing_dates;
create table monitoring.snowplow_missing_dates
(table_name varchar(200),
missing_date date);

insert into monitoring.snowplow_missing_dates
select 'atomic.events',datum 
from public.dim_dates 
where datum not in (select cast(collector_tstamp as date) from "atomic".events) 
and datum < getdate() 
and datum > (select cast(min(collector_tstamp) as date) from "atomic".events);

insert into monitoring.snowplow_missing_dates
select 'web.pageviews',datum 
from public.dim_dates 
where datum not in (select cast(page_view_date as date) from web.page_views_snowplow) 
and datum < getdate() 
and datum > (select cast(min(page_view_date) as date) from web.page_views_snowplow);

GRANT SELECT ON monitoring.snowplow_missing_dates TO tableau;
