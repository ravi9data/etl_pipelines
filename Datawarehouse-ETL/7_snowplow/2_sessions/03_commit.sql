BEGIN;

  DROP TABLE IF EXISTS web.page_views_previous_day;
  create table web.page_views_previous_day AS
      select root_id, anonymous_id, encoded_customer_id, customer_id, user_registration_date, customer_acquisition_date, customer_id_mapped, session_id, session_index, page_view_id, page_view_index, page_view_in_session_index, page_view_date, page_view_start, page_view_end, page_view_start_local, page_view_end_local, login_status, time_engaged_in_s, time_engaged_in_s_tier,  user_bounced, user_engaged, page_url, page_urlpath, page_title, page_type, page_type_detail, page_width, page_height, store_id, store_name, store_label, referer_url, referer_url_host, referer_medium, referer_source, referer_term, marketing_medium, marketing_source, marketing_term, marketing_content, marketing_campaign, marketing_click_id, marketing_network, geo_country, browser, browser_family, browser_language, platform, os, os_family, os_timezone, device, device_type, device_is_mobile 
      from web.page_views_snowplow
      where page_view_date = current_date - 1;

COMMIT;

BEGIN;

drop table if exists  web.sessions_previous_day;
create table web.sessions_previous_day
as
select * from web.sessions_snowplow
where session_start::date = current_date - 1;

COMMIT;

GRANT SELECT ON web.page_views_previous_day TO tableau;
GRANT SELECT ON web.sessions_previous_day TO tableau;
