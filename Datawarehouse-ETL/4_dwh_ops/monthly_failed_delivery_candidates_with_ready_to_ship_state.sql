drop table if exists dm_operations.monthly_failed_delivery_candidates_with_ready_to_ship_state;
create table dm_operations.monthly_failed_delivery_candidates_with_ready_to_ship_state as

WITH monthly_failed_delivery_candidates_with_ready_to_ship_state AS(
SELECT 
date_trunc('month',shipment_label_created_at)::date AS fact_date
,warehouse
,customer_type
,carrier
,count(DISTINCT ob_shipment_unique) AS failed_deliveries_w_ready_to_ship
,count(DISTINCT CASE WHEN replacement_date IS NOT NULL THEN ob_shipment_unique END) AS replacements_of_rts_cases
FROM ods_operations.failed_delivery_candidates_with_ready_to_ship_state
GROUP BY 1,2,3,4
)

,monthly_total_shipments as(
SELECT date_trunc('month',fact_date)::date AS fact_date
,warehouse
,customer_type
,carrier
,sum(total_shipments) AS total_shipments
FROM dm_operations.shipments_daily sd
WHERE shipment_type ='outbound'
AND warehouse IS NOT NULL AND warehouse<>'' 
AND fact_date IS NOT NULL 
AND fact_date>='2023-01-01'
GROUP BY 1,2,3,4)

--CTE
SELECT f.*
,t.total_shipments
FROM monthly_failed_delivery_candidates_with_ready_to_ship_state f
LEFT JOIN monthly_total_shipments t 
ON f.fact_date =t.fact_date AND f.warehouse=t.warehouse AND f.customer_type=t.customer_type AND f.carrier=t.carrier;

GRANT SELECT ON dm_operations.monthly_failed_delivery_candidates_with_ready_to_ship_state TO tableau;
