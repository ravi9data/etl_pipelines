
CREATE TEMP TABLE tmp_segment_sessions_incremental_app AS
WITH last_3_days_sessions AS (
    SELECT DISTINCT
        context_actions_amplitude_session_id::VARCHAR AS session_id
    FROM react_native.screens
    WHERE timestamp::DATE >= CURRENT_DATE - 3
),

session_main AS (
    SELECT
        session_id,
        MIN(page_view_start) AS session_start,
        MAX(page_view_end) AS session_end,
        COUNT(*) AS page_views,
        SUM(time_engaged_in_s) AS time_engaged_in_s,
        SUM(CASE WHEN user_bounced THEN 1 ELSE 0 END) AS bounced_page_views,
        SUM(CASE WHEN user_engaged THEN 1 ELSE 0 END) AS engaged_page_views
    FROM segment.page_views_app
        INNER JOIN last_3_days_sessions USING(session_id)
    GROUP BY 1
),

session_additional_info AS (
    SELECT
        context_actions_amplitude_session_id::VARCHAR AS session_id,
        FIRST_VALUE(context_ip) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ip_address,
        FIRST_VALUE(context_timezone) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS timezone,
        FIRST_VALUE(NULLIF(store_id,'-1')) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS store_id,
        FIRST_VALUE(locale) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS locale,
        ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY timestamp) AS rn
    FROM react_native.screens a
       INNER JOIN session_main b on a.context_actions_amplitude_session_id::VARCHAR = b.session_id
)

SELECT
    b.anonymous_id,
    NULL AS encoded_customer_id,
    COALESCE(b.customer_id,b.customer_id_mapped)::VARCHAR AS customer_id,
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
    NULL is_qa_url,
    b.page_type AS first_page_type,
    'Direct' AS marketing_channel,
    'Grover App' AS marketing_medium,
    NULL AS marketing_source,
    NULL AS marketing_campaign,
    NULL AS marketing_term,
    NULL AS referer_url,
    NULL AS marketing_content,
    NULL AS marketing_click_id,
    b.marketing_network,
    NULL AS is_voucher_join,
    d.store_id,
    s.store_label,
    s.store_name,
    b.geo_country,
    b.geo_region_name,
    b.geo_city,
    b.geo_zipcode,
    b.geo_latitude,
    b.geo_longitude,
    d.timezone AS geo_timezone,
    d.ip_address,
    NULL AS ip_isp,
    NULL AS ip_organization,
    NULL AS ip_domain,
    NULL AS ip_net_speed,
    NULL AS browser,
    b.platform,
    d.locale AS browser_language,
    b.os_family AS os,
    b.geo_timezone as os_timezone,
    b.device,
    b.device_type,
    b.device_is_mobile
FROM session_main a
         INNER JOIN segment.page_views_app b ON a.session_id = b.session_id
         LEFT JOIN session_additional_info d ON d.session_id = a.session_id AND d.rn = 1
         LEFT JOIN ods_production.store s ON d.store_id = s.id
WHERE b.page_view_in_session_index = 1;

BEGIN transaction;

DELETE FROM segment.sessions_app
    USING tmp_segment_sessions_incremental_app b
WHERE sessions_app.session_id = b.session_id;

INSERT INTO segment.sessions_app
SELECT *
FROM tmp_segment_sessions_incremental_app;

END transaction;

BEGIN transaction;

DELETE FROM traffic.sessions
    USING tmp_segment_sessions_incremental_app b
WHERE sessions.session_id = b.session_id AND sessions.traffic_source = 'segment_app';

INSERT INTO traffic.sessions
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
    'segment_app'::VARCHAR AS traffic_source
FROM tmp_segment_sessions_incremental_app a
    LEFT JOIN segment.sessions_web b USING(session_id)
WHERE b.session_id IS NULL;

END transaction;

DROP TABLE IF EXISTS tmp_segment_sessions_incremental_app;
