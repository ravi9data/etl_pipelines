DROP TABLE IF EXISTS ods_production.order_previous_order_history;

CREATE TABLE ods_production.order_previous_order_history AS
SELECT
    o.order_id AS main_order_id,
    o2.order_id AS related_order_id,
    o2.status AS related_order_status,
    o2.created_date AS related_created_at,
    o2.submitted_date AS related_submitted_at,
    o2.ordered_products AS related_product_name,
    o2.ordered_plan_durations AS related_plan_duration,
    o2.ordered_quantities AS related_quantity
FROM ods_production."order" AS o
LEFT JOIN ods_production."order" AS o2 ON o.customer_id = o2.customer_id
WHERE o2.created_date >= o.created_date::DATE - 5
  AND main_order_id != related_order_id
  AND o.status IN ('MANUAL REVIEW')
ORDER BY o.customer_id, o2.created_date
;


DROP TABLE IF EXISTS ods_production.order_manual_review_previous_order_history;

CREATE TABLE ods_production.order_manual_review_previous_order_history AS
WITH agg AS (
        SELECT
            main_order_id,
            '{"related_order_id": "' || related_order_id || '", "related_order_status": "' ||
            related_order_status || '", "related_created_at": "' || related_created_at ||
            '", "related_submitted_at": "' || COALESCE(related_submitted_at,current_Date) ||
            '", "related_product_name": "' || COALESCE(REPLACE(related_product_name, '"', ''), 'none') || '"}' AS ord
        FROM ods_production.order_previous_order_history
        )
SELECT
    agg.main_order_id,
    '[' || LISTAGG(agg.ord, ', ') WITHIN GROUP (ORDER BY agg.main_order_id)|| ']' AS related_order_ids
FROM agg
GROUP BY 1
;


DROP TABLE ods_production.order_previous_order_history;
