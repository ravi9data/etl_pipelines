CREATE TEMP TABLE tmp_web_session_increment
  DISTKEY(session_id)
  SORTKEY(session_id)
AS 
WITH 
prep AS (
  SELECT
    session_id,
    max(case when page_url like '%/join%' then 1 else 0 end) as is_voucher_join,
    MIN(page_view_start) AS session_start,
    MAX(page_view_end) AS session_end,
    MIN(page_view_start_local) AS session_start_local,
    MAX(page_view_end_local) AS session_end_local,
    COUNT(*) AS page_views,
    SUM(time_engaged_in_s) AS time_engaged_in_s,
    SUM(CASE WHEN user_bounced THEN 1 ELSE 0 END) AS bounced_page_views,
    SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views
  FROM web.page_views_snowplow
  GROUP BY 1
  HAVING DATE(session_start) > DATEADD('week', -1, CURRENT_DATE)
  ),

  sums AS (
  SELECT
    a.snowplow_user_id,
    a.encoded_customer_id,
    a.customer_id,
    a.session_id,
    a.session_index,
    a.page_view_index,
    b.session_start,
    b.session_end,
    TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI:SS') AS session_time,
    TO_CHAR(b.session_start, 'YYYY-MM-DD HH24:MI') AS session_minute,
    TO_CHAR(b.session_start, 'YYYY-MM-DD HH24') AS session_hour,
    TO_CHAR(b.session_start, 'YYYY-MM-DD') AS session_date,
    TO_CHAR(DATE_TRUNC('week', b.session_start), 'YYYY-MM-DD') AS session_week,
    TO_CHAR(b.session_start, 'YYYY-MM') AS session_month,
    TO_CHAR(DATE_TRUNC('quarter', b.session_start), 'YYYY-MM') AS session_quarter,
    DATE_PART(Y, b.session_start)::INTEGER AS session_year,
    b.session_start_local,
    b.session_end_local,
    TO_CHAR(b.session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS session_local_time,
    TO_CHAR(b.session_start_local, 'HH24:MI') AS session_local_time_of_day,
    DATE_PART(hour, b.session_start_local)::INTEGER AS session_local_hour_of_day,
    TRIM(TO_CHAR(b.session_start_local, 'd')) AS session_local_day_of_week,
    MOD(EXTRACT(DOW FROM b.session_start_local)::INTEGER - 1 + 7, 7) AS session_local_day_of_week_index,
    b.page_views,
    b.bounced_page_views,
    b.engaged_page_views,
    b.time_engaged_in_s,
    CASE
      WHEN b.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
      WHEN b.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
      WHEN b.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
      WHEN b.time_engaged_in_s BETWEEN 60 AND 119 THEN '60s to 119s'
      WHEN b.time_engaged_in_s BETWEEN 120 AND 239 THEN '120s to 239s'
      WHEN b.time_engaged_in_s > 239 THEN '240s or more'
      ELSE NULL
    END AS time_engaged_in_s_tier,
    CASE 
     WHEN (b.page_views = 1 AND b.bounced_page_views = 1) 
     THEN TRUE 
     ELSE FALSE 
    END AS user_bounced,
    CASE 
     WHEN (b.page_views > 2 AND b.time_engaged_in_s > 59) 
      OR b.engaged_page_views > 0
     THEN TRUE 
    ELSE FALSE 
    END AS user_engaged,
    a.page_title AS first_page_title,
    a.page_url AS first_page_url,
    case when a.page_url like ('%frontqa%') then true else false end is_qa_url,
    a.page_type as first_page_type,
    a.page_type_detail as first_page_type_detail,
    m.is_paid,
    case 
    when m.marketing_channel in (
    'Others',
    'Internal',
    'n/a'
    ) then 'Other'
    ELSE COALESCE(m.marketing_channel,'n/a') 
    END as marketing_channel,
    m.marketing_medium,
    m.marketing_source,
    m.marketing_campaign,
    m.marketing_term,
    m.referer_url,
    a.marketing_content,
    a.marketing_click_id,
    a.marketing_network,
    b.is_voucher_join,
    a.store_id,
    a.store_label,
    a.store_name,
    a.geo_country,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone,
    a.ip_address,
    a.ip_isp,
    a.ip_organization,
    a.ip_domain,
    a.ip_net_speed,
    a.browser_family as browser,
    a.platform,
    a.browser_language,
    a.os_family as os,
    a.os_timezone,
    a.device,
    a.device_type as device_type,
    a.device_is_mobile
  FROM prep AS b
  INNER JOIN web.page_views_snowplow AS a
    ON a.session_id = b.session_id
  LEFT JOIN web.session_marketing_mapping_snowplow m
   on m.session_id=b.session_id
  WHERE a.page_view_in_session_index = 1)

  SELECT
    snowplow_user_id,
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
    first_page_type_detail,
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
    device_is_mobile
  FROM sums;

BEGIN transaction;

DELETE FROM web.sessions_snowplow
USING tmp_web_session_increment b
WHERE sessions_snowplow.session_id = b.session_id;

INSERT INTO web.sessions_snowplow
SELECT * FROM tmp_web_session_increment;

END transaction;

DROP TABLE tmp_web_session_increment;
