DROP TABLE IF EXISTS segment.session_order_user_mapping_web;
CREATE TABLE segment.session_order_user_mapping_web AS
SELECT 
    a.session_id,
    o.order_id,
    MIN(a.page_view_start) AS session_start
FROM segment.page_views_web a
    INNER JOIN ods_production.order o ON o.customer_id = COALESCE(a.customer_id::INT, a.customer_id_mapped)
        AND a.page_view_start::DATE BETWEEN DATE_ADD('day', -30, o.created_date::DATE)
        AND COALESCE(o.submitted_date, o.created_date)::DATE
GROUP BY 1,2;

DROP TABLE IF EXISTS segment.session_order_user_mapping_app;
CREATE TABLE segment.session_order_user_mapping_app AS
SELECT 
    a.session_id,
    o.order_id,
    MIN(a.session_start) AS session_start
FROM segment.sessions_app a
         INNER JOIN ods_production.order o ON o.customer_id = a.customer_id
            AND session_start::DATE BETWEEN DATE_ADD('day', -30, o.created_date::DATE)
            AND COALESCE(o.submitted_date, o.created_date)::DATE
GROUP BY 1,2;

GRANT SELECT ON segment.session_order_user_mapping_web TO tableau;
GRANT SELECT ON segment.session_order_user_mapping_app TO tableau;
GRANT SELECT ON segment.session_order_user_mapping_web TO hams;
GRANT SELECT ON segment.session_order_user_mapping_app TO hams;
