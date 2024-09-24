CREATE TEMP TABLE tmp_segment_order_conversions AS
WITH new_sessions AS (
    SELECT DISTINCT
        a.session_id
    FROM segment.all_events a
        INNER JOIN segment.sessions_web b USING (session_id)
    WHERE a.platform = 'web'
      AND a.loaded_at >= CURRENT_DATE - 30
    
    UNION
    
    SELECT DISTINCT
        a.context_actions_amplitude_session_id::VARCHAR AS session_id
    FROM react_native.screens a
        INNER JOIN segment.sessions_app b 
          ON a.context_actions_amplitude_session_id::VARCHAR = b.session_id
    WHERE a.timestamp >= CURRENT_DATE - 30
),

orders AS (
    SELECT DISTINCT 
        order_id
    FROM traffic.session_order_mapping
    INNER JOIN new_sessions USING(session_id)
    
    UNION 
    
    SELECT DISTINCT o.order_id
    FROM ods_production.ORDER o
    WHERE o.submitted_date::date >= DATEADD('day',-5,current_date ) -- TO CHECK ALSO the orders that were submitted IN the LAST 5 days
),

last_mkt_data_non_direct AS (
    SELECT
        order_id,
        MAX(session_rank_order_excl_direct) AS max_session_rank_order_excl_direct
    FROM traffic.session_order_mapping
    GROUP BY 1
)

SELECT
    sm.order_id,
    NULL AS geo_cities,
    NULL AS no_of_geo_cities,
    NULL AS mietkauf_tooltip_events,
    NULL AS failed_delivery_tracking_events,
    MIN(sm.session_start) AS min_session_start,
    NULL AS search_enter_events,
    NULL AS min_search_enter_timestamp,
    NULL AS search_confirm_events,
    NULL AS search_exit_events,
    NULL AS availability_service,
    MAX(sm.session_count_order) AS touchpoints,
    MAX(CASE WHEN sm.first_touchpoint_30d
        THEN sm.marketing_channel
    END) AS first_touchpoint_30d,
        MAX(CASE WHEN sm.first_touchpoint
                     THEN sm.marketing_channel
            END) AS first_touchpoint,
    MAX(CASE WHEN sm.session_rank_order = sm.session_count_order
                 THEN sm.marketing_channel
        END) AS last_touchpoint,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
                          THEN sm.marketing_channel
        END), last_touchpoint) AS last_touchpoint_excl_direct,
    MAX(CASE WHEN sm.session_rank_order = 1
                 THEN sm.marketing_source
        END) AS first_touchpoint_mkt_source,
    MAX(CASE WHEN sm.first_touchpoint_30d
                 THEN sm.marketing_source
        END) AS first_touchpoint_30d_mkt_source,
    MAX(CASE WHEN sm.session_rank_order = sm.session_count_order
                 THEN sm.marketing_source
        END) AS last_touchpoint_mkt_source,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
                          THEN sm.marketing_source
        END),last_touchpoint_mkt_source) AS last_touchpoint_excl_direct_mkt_source,
    MAX(CASE WHEN sm.session_rank_order = 1
                 THEN sm.marketing_medium
        END) AS first_touchpoint_mkt_medium,
    MAX(CASE WHEN sm.first_touchpoint_30d
                 THEN sm.marketing_medium
        END) AS first_touchpoint_30d_mkt_medium,
    MAX(CASE WHEN sm.session_rank_order = sm.session_count_order
                 THEN sm.marketing_medium
        END) AS last_touchpoint_mkt_medium,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
                          THEN sm.marketing_medium
        END),last_touchpoint_mkt_medium) AS last_touchpoint_excl_direct_mkt_medium,
    COALESCE(MAX(CASE WHEN sm.last_touchpoint_before_submitted
                          THEN sm.os
        END),'n/a') AS os,
    NULL AS browser,
    NULL AS last_touchpoint_checkout_flow,
    LISTAGG(DISTINCT sm.marketing_channel, ' -> ')
      WITHIN GROUP (ORDER BY sm.session_rank_order) AS customer_journey,
    LISTAGG(DISTINCT sm.marketing_channel, ' -> ')
      WITHIN GROUP (ORDER BY sm.session_rank_order) AS unique_customer_journey,
    NULL AS customer_journey_checkout_flow,
    COALESCE(MAX(CASE WHEN sm.last_touchpoint_before_submitted
        THEN sm.device_type
    END), 'n/a') AS last_touchpoint_device_type,
    COALESCE(MAX(CASE WHEN sm.last_touchpoint_before_submitted
        THEN t.referer_url
    END),'n/a') AS last_touchpoint_referer_url,
    COALESCE(MAX(CASE WHEN sm.last_touchpoint_before_submitted
    THEN t.marketing_campaign
        END),'n/a') AS last_touchpoint_marketing_campaign,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
    THEN t.marketing_campaign
        END),'n/a') AS last_touchpoint_excl_direct_marketing_campaign,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
    THEN t.marketing_content
        END),'n/a') AS last_touchpoint_excl_direct_marketing_content,
    COALESCE(MAX(CASE WHEN sm.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct
    THEN t.marketing_term
    END),'n/a') AS last_touchpoint_excl_direct_marketing_term
FROM traffic.session_order_mapping sm
    INNER JOIN orders o 
        ON sm.order_id = o.order_id
    LEFT JOIN traffic.sessions t
        ON sm.session_id = t.session_id
    LEFT JOIN last_mkt_data_non_direct lm
        ON sm.order_id = lm.order_id
GROUP BY 1;

BEGIN TRANSACTION;

DELETE FROM traffic.order_conversions 
USING tmp_segment_order_conversions b
WHERE order_conversions.order_id = b.order_id;

INSERT INTO traffic.order_conversions 
SELECT * 
FROM tmp_segment_order_conversions;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_segment_order_conversions;
