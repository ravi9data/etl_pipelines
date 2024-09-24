drop table if exists web.order_info;
CREATE table web.order_info as(
select a.*, b."action",
ev.user_id,
ev.domain_userid,
ev.domain_sessionid,
ev.domain_sessionidx,
ev.mkt_campaign,
ev.mkt_clickid,
ev.mkt_content,
ev.mkt_medium,
ev.mkt_network,
ev.mkt_source,
ev.mkt_term,
ev.geo_city,
ev.geo_country,
ev.geo_region,
ev.geo_region_name,
ev.os_family,
ev.os_name,
ev.page_url
from atomic.com_google_analytics_enhanced_ecommerce_action_field_object_1 as a
join atomic.com_google_analytics_enhanced_ecommerce_action_1 as b 
   on (a.root_id) = (b.root_id)
join atomic.events as ev 
   on (a.root_id) = (ev.event_id)
where ev.useragent not like '%Datadog%'
);