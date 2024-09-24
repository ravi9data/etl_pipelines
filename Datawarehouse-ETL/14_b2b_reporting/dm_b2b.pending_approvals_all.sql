DROP VIEW IF EXISTS dm_b2b.pending_approvals_all;
CREATE VIEW dm_b2b.pending_approvals_all  AS  
SELECT distinct 
  af.customer_id
 ,c.company_name
 ,c.company_type_name
 ,af.order_id
 ,af.submitted_date::DATE
 ,af.order_value, af.new_recurring
 ,DATEDIFF('day',af.submitted_date::DATE, CURRENT_DATE) AS days_since_submitted
 ,burgel_risk_category
 ,order_journey_mapping_risk as risk_status
 ,c.status as company_status
 ,o.status
 ,o.declined_reason
FROM dm_risk.approval_funnel af 
  LEFT JOIN ods_production.companies c 
    ON c.customer_id = af.customer_id 
  LEFT JOIN ods_production.order o 
    ON o.order_id = af.order_id 
WHERE TRUE
  AND af.completed_orders  = 1 
  AND af.order_journey_mapping_risk in ('Bank Account Snapshot', 'Onfido', 'Pending', 'Manual Review')
  AND af.submitted_date::DATE <= CURRENT_DATE 
  AND af.submitted_date::DATE >= DATE_TRUNC('MONTH', DATEADD('MONTH', -12, CURRENT_DATE))
  AND af.store_commercial in ('B2B Germany', 'B2B International')
  WITH NO SCHEMA BINDING
;