drop table if exists scratch.url_details;
create table scratch.url_details as 
select distinct 
    ev.page_urlhost || ev.page_urlpath AS page_url,
    ev.page_urlscheme AS page_url_scheme,
    ev.page_urlhost AS page_url_host,
    ev.page_urlport AS page_url_port,
    ev.page_urlpath AS page_url_path,
    ev.page_urlquery AS page_url_query,
    ev.page_urlfragment AS page_url_fragment,
    ev.page_title,
count(*) as pageviews,
count(distinct page_view_id) as pageviews_validate,
count(distinct domain_sessionid) as sessions,
count(distinct domain_userid) as web_users,
count(distinct user_id) as customers
--select *
FROM scratch.web_events AS ev
--limit 10;
group by 1,2,3,4,5,6,7,8
order by count(distinct user_id) desc 
;