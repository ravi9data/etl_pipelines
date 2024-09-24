DROP TABLE IF EXISTS traffic.order_conversions;

CREATE TABLE traffic.order_conversions AS
WITH last_mkt_data_non_direct AS (
SELECT
    order_id,
    MAX(session_rank_order_excl_direct) max_session_rank_order_excl_direct
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
    LEFT JOIN traffic.sessions t
        ON sm.session_id = t.session_id
    LEFT JOIN last_mkt_data_non_direct lm
        ON sm.order_id = lm.order_id
GROUP BY 1;

GRANT SELECT ON traffic.order_conversions TO tableau;
GRANT SELECT ON traffic.order_conversions TO redash_growth;
GRANT SELECT ON traffic.order_conversions TO ceyda_peker;
