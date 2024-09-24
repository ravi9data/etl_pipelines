
CREATE TEMP TABLE tmp_segment_sessions_incremental_web AS
WITH last_3_days_sessions AS (
    SELECT DISTINCT
        session_id
    FROM segment.all_events
    WHERE platform = 'web'
    AND loaded_at >= CURRENT_DATE - 3
),

session_main AS (
    SELECT
        session_id,
        MAX(CASE WHEN page_url LIKE '%/join%' THEN 1 ELSE 0 END) AS is_voucher_join,
        MIN(page_view_start) AS session_start,
        MAX(page_view_end) AS session_end,
        COUNT(*) AS page_views,
        SUM(time_engaged_in_s) AS time_engaged_in_s,
        SUM(CASE WHEN user_bounced THEN 1 ELSE 0 END) AS bounced_page_views,
        SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views
    FROM segment.page_views_web
    INNER JOIN last_3_days_sessions USING (session_id)
    GROUP BY 1
),

session_non_page_info AS (
    SELECT
        session_id,
        FIRST_VALUE(ip) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ip_address,
        FIRST_VALUE(click_id) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS gclid,
        FIRST_VALUE(timezone) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS timezone,
        FIRST_VALUE(locale) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS locale,
        ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_time) AS rn
    FROM segment.all_events
        INNER JOIN session_main USING(session_id)
    WHERE platform = 'web'
)

SELECT
    b.anonymous_id,
    NULL AS encoded_customer_id,
    COALESCE(b.customer_id,b.customer_id_mapped) AS customer_id,
    a.session_id,
    b.session_index,
    b.page_view_index,
    a.session_start,
    a.session_end,
    TO_CHAR(a.session_start, 'YYYY-MM-DD HH24:MI:SS') AS session_time,
    TO_CHAR(a.session_start, 'YYYY-MM-DD HH24:MI') AS session_minute,
    TO_CHAR(a.session_start, 'YYYY-MM-DD HH24') AS session_hour,
    TO_CHAR(a.session_start, 'YYYY-MM-DD') AS session_date,
    TO_CHAR(DATE_TRUNC('week', a.session_start), 'YYYY-MM-DD') AS session_week,
    TO_CHAR(a.session_start, 'YYYY-MM') AS session_month,
    TO_CHAR(DATE_TRUNC('quarter', a.session_start), 'YYYY-MM') AS session_quarter,
    DATE_PART(Y, a.session_start)::INTEGER AS session_year,
    NULL AS session_start_local,
    NULL AS session_end_local,
    TO_CHAR(a.session_start, 'YYYY-MM-DD HH24:MI:SS') AS session_local_time,
    TO_CHAR(a.session_start, 'HH24:MI') AS session_local_time_of_day,
    DATE_PART(hour, a.session_start)::INTEGER AS session_local_hour_of_day,
    TRIM(TO_CHAR(a.session_start, 'd')) AS session_local_day_of_week,
    MOD(EXTRACT(DOW FROM a.session_start)::INTEGER - 1 + 7, 7) AS session_local_day_of_week_index,
    a.page_views,
    a.bounced_page_views,
    a.engaged_page_views,
    a.time_engaged_in_s,
    CASE
        WHEN a.time_engaged_in_s BETWEEN 0 AND 9 THEN '0s to 9s'
        WHEN a.time_engaged_in_s BETWEEN 10 AND 29 THEN '10s to 29s'
        WHEN a.time_engaged_in_s BETWEEN 30 AND 59 THEN '30s to 59s'
        WHEN a.time_engaged_in_s BETWEEN 60 AND 119 THEN '60s to 119s'
        WHEN a.time_engaged_in_s BETWEEN 120 AND 239 THEN '120s to 239s'
        WHEN a.time_engaged_in_s > 239 THEN '240s or more'
        ELSE NULL
        END AS time_engaged_in_s_tier,
    CASE
        WHEN (a.page_views = 1 AND a.bounced_page_views = 1)
            THEN TRUE
        ELSE FALSE
        END AS user_bounced,
    CASE
        WHEN (a.page_views > 2 AND a.time_engaged_in_s > 59)
            OR a.engaged_page_views > 0
            THEN TRUE
        ELSE FALSE
        END AS user_engaged,
    b.page_title AS first_page_title,
    b.page_url AS first_page_url,
    CASE WHEN b.page_url LIKE ('%frontqa%') THEN TRUE ELSE FALSE END is_qa_url,
    b.page_type AS first_page_type,
    CASE WHEN c.marketing_channel IN
              ('Display Branding',
               'Display Performance',
               'Paid Search Brand',
               'Paid Search Non Brand',
               'Paid Social Branding',
               'Paid Social Performance',
               'Shopping',
               'Partnerships',
               'Offline Partnerships',
               'Affiliates',
               'Influencers',
               'Refer Friend',
               'Podcasts',
               'Sponsorships',
               'Retail')
             THEN TRUE
         ELSE FALSE
        END AS is_paid,
    CASE
        WHEN c.marketing_channel IN ('Others', 'Internal','n/a')
            THEN 'Other'
        ELSE COALESCE(c.marketing_channel,'n/a')
        END AS marketing_channel,
    c.marketing_medium,
    c.marketing_source,
    c.marketing_campaign,
    c.marketing_term,
    c.page_referrer AS referer_url,
    c.marketing_content,
    d.gclid AS marketing_click_id,
    b.marketing_network,
    a.is_voucher_join,
    b.store_id,
    b.store_label,
    b.store_name,
    b.geo_country,
    b.geo_region_name,
    b.geo_city,
    b.geo_zipcode,
    b.geo_latitude,
    b.geo_longitude,
    d.timezone AS geo_timezone,
    d.ip_address,
    b.ip_isp,
    b.ip_organization,
    b.ip_domain,
    b.ip_net_speed,
    b.browser,
    b.platform,
    d.locale AS browser_language,
    b.os_family AS os,
    b.os_timezone,
    b.device,
    b.device_type,
    b.device_is_mobile
FROM session_main a
         INNER JOIN segment.page_views_web b ON a.session_id = b.session_id
         LEFT JOIN segment.session_marketing_mapping_web c ON c.session_id = a.session_id
         LEFT JOIN session_non_page_info d ON d.session_id = a.session_id AND d.rn = 1
WHERE b.page_view_in_session_index = 1;

BEGIN transaction;

DELETE FROM segment.sessions_web
    USING tmp_segment_sessions_incremental_web b
WHERE sessions_web.session_id = b.session_id;

INSERT INTO segment.sessions_web
SELECT *
FROM tmp_segment_sessions_incremental_web;

END transaction;

BEGIN transaction;

DELETE FROM traffic.sessions
    USING tmp_segment_sessions_incremental_web b
WHERE sessions.session_id = b.session_id;

INSERT INTO traffic.sessions
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
    'segment_web'::VARCHAR AS traffic_source
FROM tmp_segment_sessions_incremental_web;

END transaction;

DROP TABLE IF EXISTS tmp_segment_sessions_incremental_web;
