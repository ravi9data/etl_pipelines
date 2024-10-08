---web_events
create temp table web_events_dl
(
    like scratch.web_events
);

insert into web_events_dl(
	user_id,
	domain_userid,
	network_userid,
	domain_sessionid,
	domain_sessionidx,
	root_id,
	page_view_id,
	page_title,
	page_url,
	page_urlscheme,
	page_urlhost,
	page_urlport,
	page_urlpath,
	page_urlquery,
	page_urlfragment,
	refr_urlscheme,
	refr_urlhost,
	refr_urlport,
	refr_urlpath,
	refr_urlquery,
	refr_urlfragment,
	refr_medium,
	refr_source,
	refr_term,
	mkt_medium,
	mkt_source,
	mkt_term,
	mkt_content,
	mkt_campaign,
	mkt_clickid,
	mkt_network,
	geo_country,
	geo_region,
	geo_region_name,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_timezone,
	user_ipaddress,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_netspeed,
	app_id,
	useragent,
	br_name,
	br_family,
	br_version,
	br_type,
	br_renderengine,
	br_lang,
	platform,
	dvce_type,
	dvce_ismobile,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	name_tracker,
	etl_tstamp,
	dvce_created_tstamp,
	n)
WITH prep AS (
    SELECT
    	TRIM(ev.user_id)                   AS user_id,
		TRIM(ev.domain_userid)             AS domain_userid,
		TRIM(ev.network_userid)            AS network_userid,
		TRIM(ev.domain_sessionid)          as domain_sessionid,
		ev.domain_sessionidx,
		wp.root_id                         AS ROOT_ID,
		TRIM(wp.page_view_id)              AS page_view_id,

		ev.page_title,
		ev.page_urlhost || ev.page_urlpath AS page_url,
		ev.page_urlscheme,
		ev.page_urlhost,
		ev.page_urlport,
		ev.page_urlpath,
		ev.page_urlquery,
		ev.page_urlfragment,

		ev.refr_urlscheme,
		ev.refr_urlhost,
		ev.refr_urlport,
		ev.refr_urlpath,
		ev.refr_urlquery,
		ev.refr_urlfragment,

		ev.refr_medium,
		ev.refr_source,
		ev.refr_term,
		ev.mkt_medium,
		ev.mkt_source,
		ev.mkt_term,
		ev.mkt_content,
		ev.mkt_campaign,
		ev.mkt_clickid,
		ev.mkt_network,

		ev.geo_country,
		ev.geo_region,
		ev.geo_region_name,
		ev.geo_city,
		ev.geo_zipcode,
		ev.geo_latitude,
		ev.geo_longitude,
		ev.geo_timezone,

		ev.user_ipaddress,

		ev.ip_isp,
		ev.ip_organization,
		ev.ip_domain,
		ev.ip_netspeed,

		ev.app_id,


		ev.useragent,
		ev.br_name,
		ev.br_family,

		ev.br_version,
		ev.br_type,
		ev.br_renderengine,
		ev.br_lang,
		ev.platform,
		ev.dvce_type,
		ev.dvce_ismobile,
		ev.os_name,
		ev.os_family,
		ev.os_manufacturer,
		ev.os_timezone,
		ev.name_tracker,       -- included to filter on
		ev.etl_tstamp,
		ev.dvce_created_tstamp -- included to sort on
    FROM atomic.events AS ev
             INNER JOIN scratch.web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
                        ON ev.event_id = wp.root_id
    WHERE (ev.event_name = 'page_view')-- filtering on page view events removes the need for a FIRST_VALUE function
      and ev.name_tracker not in ('snplow1')
      and ev.etl_tstamp::date > DATEADD(week, -1, CURRENT_DATE)
      and ev.useragent not like '%Datadog%' 
), order_data AS(
    SELECT
    	user_id,
		domain_userid,
		network_userid,
		domain_sessionid,
		domain_sessionidx,
		root_id,
		page_view_id,
		page_title,
		page_url,
		page_urlscheme,
		page_urlhost,
		page_urlport,
		page_urlpath,
		page_urlquery,
		page_urlfragment,
		refr_urlscheme,
		refr_urlhost,
		refr_urlport,
		refr_urlpath,
		refr_urlquery,
		refr_urlfragment,
		refr_medium,
		refr_source,
		refr_term,
		mkt_medium,
		mkt_source,
		mkt_term,
		mkt_content,
		mkt_campaign,
		mkt_clickid,
		mkt_network,
		geo_country,
		geo_region,
		geo_region_name,
		geo_city,
		geo_zipcode,
		geo_latitude,
		geo_longitude,
		geo_timezone,
		user_ipaddress,
		ip_isp,
		ip_organization,
		ip_domain,
		ip_netspeed,
		app_id,
		useragent,
		br_name,
		br_family,
		br_version,
		br_type,
		br_renderengine,
		br_lang,
		platform,
		dvce_type,
		dvce_ismobile,
		os_name,
		os_family,
		os_manufacturer,
		os_timezone,
		name_tracker,
		etl_tstamp,
		dvce_created_tstamp,
		ROW_NUMBER() OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp) AS n
      FROM prep
    )


SELECT
	user_id,
	domain_userid,
	network_userid,
	domain_sessionid,
	domain_sessionidx,
	root_id,
	page_view_id,
	page_title,
	page_url,
	page_urlscheme,
	page_urlhost,
	page_urlport,
	page_urlpath,
	page_urlquery,
	page_urlfragment,
	refr_urlscheme,
	refr_urlhost,
	refr_urlport,
	refr_urlpath,
	refr_urlquery,
	refr_urlfragment,
	refr_medium,
	refr_source,
	refr_term,
	mkt_medium,
	mkt_source,
	mkt_term,
	mkt_content,
	mkt_campaign,
	mkt_clickid,
	mkt_network,
	geo_country,
	geo_region,
	geo_region_name,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_timezone,
	user_ipaddress,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_netspeed,
	app_id,
	useragent,
	br_name,
	br_family,
	br_version,
	br_type,
	br_renderengine,
	br_lang,
	platform,
	dvce_type,
	dvce_ismobile,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	name_tracker,
	etl_tstamp,
	dvce_created_tstamp,
	n
FROM order_data
WHERE n = 1;


begin transaction;

delete
from scratch.web_events
    using web_events_dl s
where web_events.page_view_id = s.page_view_id;


insert into scratch.web_events(user_id, domain_userid, network_userid, domain_sessionid, domain_sessionidx, root_id,
                               page_view_id, page_title, page_url, page_urlscheme, page_urlhost, page_urlport,
                               page_urlpath, page_urlquery, page_urlfragment, refr_urlscheme, refr_urlhost,
                               refr_urlport, refr_urlpath, refr_urlquery, refr_urlfragment, refr_medium, refr_source,
                               refr_term, mkt_medium, mkt_source, mkt_term, mkt_content, mkt_campaign, mkt_clickid,
                               mkt_network, geo_country, geo_region, geo_region_name, geo_city, geo_zipcode, geo_latitude,
                               geo_longitude, geo_timezone, user_ipaddress, ip_isp, ip_organization, ip_domain,
                               ip_netspeed, app_id, useragent, br_name, br_family, br_version, br_type, br_renderengine,
                               br_lang, platform, dvce_type, dvce_ismobile, os_name, os_family, os_manufacturer,
                               os_timezone, name_tracker, etl_tstamp, dvce_created_tstamp, n)
select
	user_id,
	domain_userid,
	network_userid,
	domain_sessionid,
	domain_sessionidx,
	root_id,
	page_view_id,
	page_title,
	page_url,
	page_urlscheme,
	page_urlhost,
	page_urlport,
	page_urlpath,
	page_urlquery,
	page_urlfragment,
	refr_urlscheme,
	refr_urlhost,
	refr_urlport,
	refr_urlpath,
	refr_urlquery,
	refr_urlfragment,
	refr_medium,
	refr_source,
	refr_term,
	mkt_medium,
	mkt_source,
	mkt_term,
	mkt_content,
	mkt_campaign,
	mkt_clickid,
	mkt_network,
	geo_country,
	geo_region,
	geo_region_name,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_timezone,
	user_ipaddress,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_netspeed,
	app_id,
	useragent,
	br_name,
	br_family,
	br_version,
	br_type,
	br_renderengine,
	br_lang,
	platform,
	dvce_type,
	dvce_ismobile,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	name_tracker,
	etl_tstamp,
	dvce_created_tstamp,
	n
from web_events_dl;

end transaction;

drop table web_events_dl;