DROP TABLE IF EXISTS traffic.sessions;
CREATE TABLE traffic.sessions
  DISTKEY(session_id)
  SORTKEY(session_id) AS
WITH order_sessions AS (
    SELECT DISTINCT
        context_actions_amplitude_session_id::VARCHAR AS session_id
    FROM react_native.order_submitted

    UNION

    SELECT DISTINCT
        context_actions_amplitude_session_id::VARCHAR AS session_id
    FROM react_native.product_added_to_cart
)

SELECT 
	snowplow_user_id AS anonymous_id, 
	encoded_customer_id,
	customer_id,
	session_id,
	session_index,
	page_view_index,
	session_start,
	session_end,
	page_views,
	bounced_page_views,
	engaged_page_views,
	time_engaged_in_s,
	time_engaged_in_s_tier,
	user_bounced,
	user_engaged,
	first_page_url,
	is_qa_url,
	is_voucher_join,
	first_page_title,
	first_page_type,
	referer_url,
	is_paid,
	marketing_channel,
	marketing_medium,
	marketing_source,
	marketing_term,
	marketing_content,
	marketing_campaign,
	marketing_click_id,
	marketing_network,
	store_id,
	store_label,
	store_name,
	geo_country,
	geo_region_name,
	geo_city,
	geo_zipcode,
	geo_latitude,
	geo_longitude,
	geo_timezone,
	ip_address,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_net_speed,
	browser,
	browser_language,
	os,
	os_timezone,
	device,
	device_type,
	device_is_mobile,
	'snowplow_web' AS traffic_source
FROM web.sessions_snowplow
WHERE session_start < '2023-05-01'

UNION 

SELECT 
	anonymous_id,
	NULL::varchar AS encoded_customer_id,
	customer_id::varchar,
	session_id::varchar,
	session_index,
	page_view_index,
	session_start,
	session_end,
	page_views,
	bounced_page_views,
	engaged_page_views,
	time_engaged_in_s,
	time_engaged_in_s_tier,
	user_bounced,
	user_engaged,
	first_page_url,
	is_qa_url,
	is_voucher_join,
	first_page_title,
	first_page_type,
	referer_url,
	is_paid,
	marketing_channel,
	marketing_medium,
	marketing_source,
	marketing_term,
	marketing_content,
	marketing_campaign,
	marketing_click_id,
	marketing_network,
	store_id,
	store_label,
	store_name,
	geo_country,
	geo_region_name,
	geo_city,
	geo_zipcode,
	geo_latitude::float,
	geo_longitude::float,
	geo_timezone,
	ip_address,
	ip_isp,
	ip_organization,
	ip_domain,
	ip_net_speed,
	browser,
	browser_language,
	os,
	os_timezone,
	device,
	device_type,
	device_is_mobile,
	'segment_web' AS traffic_source
FROM segment.sessions_web
WHERE session_start >= '2023-05-01'

UNION ALL

SELECT 
	a.anonymous_id,
	NULL::varchar AS encoded_customer_id,
	a.customer_id::varchar,
	a.session_id::varchar,
	a.session_index,
	a.page_view_index,
	a.session_start,
	a.session_end,
	a.page_views,
	a.bounced_page_views,
	a.engaged_page_views,
	a.time_engaged_in_s,
	a.time_engaged_in_s_tier,
	a.user_bounced,
	a.user_engaged,
	a.first_page_url,
	FALSE AS is_qa_url,
	a.is_voucher_join::INT,
	a.first_page_title,
	a.first_page_type,
	a.referer_url,
	FALSE AS is_paid,
	a.marketing_channel,
	a.marketing_medium,
	a.marketing_source,
	a.marketing_term,
	a.marketing_content,
	a.marketing_campaign,
	a.marketing_click_id,
	a.marketing_network,
	a.store_id,
	a.store_label,
	a.store_name,
	a.geo_country,
	a.geo_region_name,
	a.geo_city,
	a.geo_zipcode,
	a.geo_latitude::float,
	a.geo_longitude::float,
	a.geo_timezone,
	a.ip_address,
	a.ip_isp,
	a.ip_organization,
	a.ip_domain,
	a.ip_net_speed,
	a.browser,
	a.browser_language,
	a.os,
	a.os_timezone,
	a.device,
	a.device_type,
	a.device_is_mobile,
	'segment_app' AS traffic_source
FROM segment.sessions_app a
	LEFT JOIN segment.sessions_web b USING(session_id)
	LEFT JOIN order_sessions c USING(session_id)
WHERE (a.session_start >= '2023-09-01' AND b.session_id IS NULL) 
	 OR (a.session_start >= '2023-05-01' AND c.session_id IS NOT NULL);

GRANT SELECT ON traffic.sessions TO tableau;
GRANT SELECT ON traffic.sessions TO redash_growth;
GRANT SELECT ON traffic.sessions TO ceyda_peker;
