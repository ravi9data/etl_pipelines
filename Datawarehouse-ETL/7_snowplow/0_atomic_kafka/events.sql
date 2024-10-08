begin transaction;
INSERT INTO atomic.events(
	app_id,
	platform,
	etl_tstamp,
	collector_tstamp,
	dvce_created_tstamp,
	event,
	event_id,
	txn_id,
	name_tracker,
	v_tracker,
	v_collector,
	v_etl,
	user_id,
	user_ipaddress,
	user_fingerprint,
	domain_userid,
	domain_sessionidx,
	network_userid,
	geo_country,
	geo_region,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_region_name,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_netspeed,
	page_url,
	page_title,
	page_referrer,
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
	se_category,
	se_action,
	se_label,
	se_property,
	se_value,
	tr_orderid,
	tr_affiliation,
	tr_total,
	tr_tax,
	tr_shipping,
	tr_city,
	tr_state,
	tr_country,
	ti_orderid,
	ti_sku,
	ti_name,
	ti_category,
	ti_price,
	ti_quantity,
	pp_xoffset_min,
	pp_xoffset_max,
	pp_yoffset_min,
	pp_yoffset_max,
	useragent,
	br_name, br_family,
	br_version, br_type,
	br_renderengine,
	br_lang,
	br_features_pdf,
	br_features_flash,
	br_features_java,
	br_features_director,
	br_features_quicktime,
	br_features_realplayer,
	br_features_windowsmedia,
	br_features_gears,
	br_features_silverlight,
	br_cookies,
	--br_colordepth,
	br_viewwidth,
	br_viewheight,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	dvce_type,
	dvce_ismobile,
	dvce_screenwidth,
	dvce_screenheight,
	doc_charset,
	doc_width,
	doc_height,
	tr_currency,
	tr_total_base,
	tr_tax_base,
	tr_shipping_base,
	ti_currency,
	ti_price_base,
	base_currency,
	geo_timezone,
	mkt_clickid,
	mkt_network,
	etl_tags,
	dvce_sent_tstamp,
	refr_domain_userid,
	refr_dvce_tstamp,
	domain_sessionid,
	derived_tstamp,
	event_vendor,
	event_name,
	event_format,
	event_version,
	event_fingerprint,
	true_tstamp
)
SELECT
	app_id,
	platform,
	etl_tstamp::timestamp AS etl_tstamp,
	collector_tstamp::timestamp AS collector_tstamp,
	dvce_created_tstamp::timestamp AS dvce_created_tstamp,
	event,
	event_id,
	txn_id::int AS txn_id,
	name_tracker,
	v_tracker,
	v_collector,
	v_etl,
	user_id,
	user_ipaddress,
	user_fingerprint,
	domain_userid,
	domain_sessionidx,
	network_userid,
	geo_country,
	geo_region,
	geo_city,
	geo_zipcode,
	geo_latitude::double precision AS geo_latitude,
	geo_longitude::double precision AS geo_longitude,
	geo_region_name,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_netspeed,
	page_url,
	page_title,
	page_referrer,
	page_urlscheme,
	page_urlhost,
	page_urlport,
	page_urlpath,
	left(page_urlquery,6000),
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
	se_category,
	se_action,
	se_label,
	se_property,
	se_value::decimal(15,2) AS se_value,
	tr_orderid,
	tr_affiliation,
	tr_total::int AS tr_total,
	tr_tax::int AS tr_tax,
	tr_shipping::int AS tr_shipping,
	tr_city,
	tr_state,
	tr_country,
	ti_orderid,
	ti_sku,
	ti_name,
	ti_category,
	ti_price::int AS ti_price,
	ti_quantity::int AS ti_quantity,
	pp_xoffset_min::int AS pp_xoffset_min,
	pp_xoffset_max::int AS pp_xoffset_max,
	pp_yoffset_min::int AS pp_yoffset_min,
	pp_yoffset_max::int AS pp_yoffset_min,
	useragent,
	br_name, br_family,
	br_version, br_type,
	br_renderengine,
	br_lang,
	DECODE(br_features_pdf,'false', '0', 'true', '1')::integer::boolean AS br_features_pdf,
	DECODE(br_features_flash,'false', '0', 'true', '1')::integer::boolean AS br_features_flash,
	DECODE(br_features_java,'false', '0', 'true', '1')::integer::boolean AS br_features_java,
	DECODE(br_features_director,'false', '0', 'true', '1')::integer::boolean AS br_features_director,
	DECODE(br_features_quicktime,'false', '0', 'true', '1')::integer::boolean AS br_features_quicktime,
	DECODE(br_features_realplayer,'false', '0', 'true', '1')::integer::boolean AS br_features_realplayer,
	DECODE(br_features_windowsmedia,'false', '0', 'true', '1')::integer::boolean AS br_features_windowsmedia,
	DECODE(br_features_gears,'false', '0', 'true', '1')::integer::boolean AS br_features_gears,
	DECODE(br_features_silverlight,'false', '0', 'true', '1')::integer::boolean AS br_features_silverlight,
	DECODE(br_cookies,'false', '0', 'true', '1')::integer::boolean AS br_cookies,
	--br_colordepth,
	br_viewwidth,
	br_viewheight,
	os_name,
	os_family,
	os_manufacturer,
	os_timezone,
	dvce_type,
	dvce_ismobile,
	dvce_screenwidth,
	dvce_screenheight,
	doc_charset,
	doc_width,
	doc_height,
	tr_currency,
	tr_total_base::int AS tr_total_base,
	tr_tax_base::int AS tr_tax_base,
	tr_shipping_base::int AS tr_shipping_base,
	ti_currency,
	ti_price_base::int AS ti_price_base,
	base_currency,
	geo_timezone,
	mkt_clickid,
	mkt_network,
	etl_tags,
	dvce_sent_tstamp::timestamp AS dvce_sent_tstamp,
	refr_domain_userid,
	refr_dvce_tstamp::timestamp AS refr_dvce_tstamp,
	domain_sessionid,
	derived_tstamp::timestamp AS derived_tstamp,
	event_vendor,
	event_name,
	event_format,
	event_version,
	event_fingerprint,
	true_tstamp::timestamp AS true_tstamp
FROM stg_kafka_events.stream_snowplow_event_parser;
end transaction;