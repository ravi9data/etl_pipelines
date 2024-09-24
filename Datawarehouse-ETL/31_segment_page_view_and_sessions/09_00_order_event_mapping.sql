DROP TABLE IF EXISTS segment.session_order_event_mapping_web;
CREATE TABLE segment.session_order_event_mapping_web AS
SELECT 
    a.session_id::VARCHAR AS session_id,
    a.order_id,
    min(a.event_time) AS event_time
FROM segment.track_events a
    INNER JOIN segment.page_views_web b ON a.session_id::VARCHAR = b.session_id
WHERE a.order_id IS NOT NULL
GROUP BY 1,2;

DROP TABLE IF EXISTS segment.session_order_event_mapping_app;
CREATE TABLE segment.session_order_event_mapping_app AS
WITH orders_info AS (
    SELECT
        context_actions_amplitude_session_id::VARCHAR AS session_id,
        order_id,
        MIN(timestamp) AS event_time
    FROM react_native.order_submitted
    GROUP BY 1,2

    UNION ALL

    SELECT
        context_actions_amplitude_session_id::VARCHAR AS session_id,
        order_id,
        MIN(timestamp) AS event_time
    FROM react_native.product_added_to_cart
    GROUP BY 1,2
)

SELECT
    a.session_id,
    a.order_id,
    min(a.event_time) AS event_time
FROM orders_info a
         INNER JOIN segment.sessions_app b ON a.session_id = b.session_id
WHERE a.order_id IS NOT NULL
GROUP BY 1,2;

GRANT SELECT ON segment.session_order_event_mapping_web TO tableau;
GRANT SELECT ON segment.session_order_event_mapping_app TO tableau;
GRANT SELECT ON segment.session_order_event_mapping_web TO hams;
GRANT SELECT ON segment.session_order_event_mapping_app TO hams;
