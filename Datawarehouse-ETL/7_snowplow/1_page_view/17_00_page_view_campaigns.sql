DROP TABLE IF EXISTS scratch.page_view_paid_search_campaigns;
CREATE TABLE scratch.page_view_paid_search_campaigns AS
WITH paid_search_sessions AS (
    SELECT DISTINCT session_id
           FROM web.session_marketing_mapping
           WHERE marketing_channel = 'Paid Search'
           AND DATE(page_view_start) BETWEEN '2021-04-01' AND  '2022-05-31'
    ),

    atomic_page_views AS (
    SELECT DISTINCT domain_sessionid AS session_id,
            event_id AS root_id,
            mkt_campaign,
            ROW_NUMBER() OVER (PARTITION BY domain_sessionid ORDER BY dvce_created_tstamp) AS rn
     FROM atomic.events a
     INNER JOIN paid_search_sessions b on domain_sessionid=session_id
     WHERE DATE(dvce_created_tstamp) >= '2021-04-01'
        AND mkt_campaign IS NOT NULL
        AND event_name = 'page_view'
        AND name_tracker NOT IN ('snplow1')
        AND useragent NOT LIKE '%Datadog%'
)

SELECT * FROM atomic_page_views;