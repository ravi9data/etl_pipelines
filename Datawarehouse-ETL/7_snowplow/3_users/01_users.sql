DROP TABLE IF EXISTS web.users_tmp;
CREATE TABLE web.users_tmp
  DISTKEY(snowplow_user_id)
  SORTKEY(first_session_start)
AS (

  WITH prep AS (

    SELECT

      snowplow_user_id,

      -- time

      MIN(session_start) AS first_session_start,
--      MIN(session_start_local) AS first_session_start_local,

      MAX(session_end) AS last_session_end,

      -- engagement

      SUM(page_views) AS page_views,
      COUNT(*) AS sessions,

      SUM(time_engaged_in_s) AS time_engaged_in_s

    FROM web.sessions_snowplow

    GROUP BY 1
    ORDER BY 1

  )

  SELECT

    -- user

    --a.user_custom_id,
    a.snowplow_user_id,
    --a.user_snowplow_crossdomain_id,
    a.customer_id,

    -- first sesssion: time

    b.first_session_start,

      -- example derived dimensions

      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI:SS') AS first_session_time,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24:MI') AS first_session_minute,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD HH24') AS first_session_hour,
      TO_CHAR(b.first_session_start, 'YYYY-MM-DD') AS first_session_date,
      TO_CHAR(DATE_TRUNC('week', b.first_session_start), 'YYYY-MM-DD') AS first_session_week,
      TO_CHAR(b.first_session_start, 'YYYY-MM') AS first_session_month,
      TO_CHAR(DATE_TRUNC('quarter', b.first_session_start), 'YYYY-MM') AS first_session_quarter,
      DATE_PART(Y, b.first_session_start)::INTEGER AS first_session_year,

    -- first session: time in the user's local timezone

--    b.first_session_start_local,

      -- example derived dimensions

/*      TO_CHAR(b.first_session_start_local, 'YYYY-MM-DD HH24:MI:SS') AS first_session_local_time,
      TO_CHAR(b.first_session_start_local, 'HH24:MI') AS first_session_local_time_of_day,
      DATE_PART(hour, b.first_session_start_local)::INTEGER AS first_session_local_hour_of_day,
      TRIM(TO_CHAR(b.first_session_start_local, 'd')) AS first_session_local_day_of_week,
      MOD(EXTRACT(DOW FROM b.first_session_start_local)::INTEGER - 1 + 7, 7) AS first_session_local_day_of_week_index,
*/

    -- last session: time

    b.last_session_end,

    -- engagement

    b.page_views,
    b.sessions,

    b.time_engaged_in_s,

    -- first page

    a.first_page_url,
/*
    a.first_page_url_scheme,
    a.first_page_url_host,
    a.first_page_url_port,
    a.first_page_url_path,
    a.first_page_url_query,
    a.first_page_url_fragment,
*/
    a.first_page_title,

    -- referer
/*
    a.referer_url,

    a.referer_url_scheme,
    a.referer_url_host,
    a.referer_url_port,
    a.referer_url_path,
    a.referer_url_query,
    a.referer_url_fragment,
    a.referer_medium,
    a.referer_source,
    a.referer_term,
    */

    -- marketing

    a.marketing_medium,
    a.marketing_source,
    a.marketing_term,
    a.marketing_content,
    a.marketing_campaign,
    a.marketing_click_id,
    a.marketing_network,
    a.marketing_channel

    -- application

   -- a.app_id

  FROM web.sessions_snowplow AS a

  INNER JOIN prep AS b
    ON a.snowplow_user_id = b.snowplow_user_id

  WHERE a.session_index = 1

);