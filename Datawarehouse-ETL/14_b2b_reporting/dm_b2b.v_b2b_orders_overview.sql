CREATE VIEW dm_b2b.v_b2b_orders_overview AS  
WITH orders AS (
    SELECT 
        sub.subscription_id,
        s.order_id,
        s.customer_id,
        a.account_name, 
        s.order_value,
        sub.subscription_value,
        s.new_recurring,
        s.submitted_date::DATE,
        s.approved_date::DATE,
        DATEDIFF('day',s.submitted_date::DATE,COALESCE(s.approved_date::DATE, CURRENT_DATE)) AS days_since_submitted_approved,
        CASE WHEN approved_date IS NULL THEN s.status
             WHEN days_since_submitted_approved = 0 THEN 'Same day approval'
             WHEN days_since_submitted_approved > 0 AND days_since_submitted_approved <= 7 THEN 'within 7 days'
             WHEN days_since_submitted_approved > 7 AND days_since_submitted_approved <= 15 THEN 'within 15 days'
             WHEN days_since_submitted_approved > 15 AND days_since_submitted_approved <= 30 THEN 'within 30 days'
             WHEN days_since_submitted_approved > 30 THEN 'More than 30 days'
        END AS approval_time,
        c.company_status,
        s.status,
        u.full_name,
        CASE WHEN u.full_name <> 'B2B Support Grover' THEN 'Sales'
             ELSE 'Self-service' 
        END AS segment
    FROM master.ORDER s
       LEFT JOIN master.subscription sub
           ON sub.order_id = s.order_id
        LEFT JOIN ods_b2b.account a
            ON s.customer_id = a.customer_id 
        LEFT JOIN ods_b2b."user" u
            ON u.user_id = a.account_owner
        LEFT JOIN master.customer c 
            ON c.customer_id = s.customer_id   
    WHERE s.customer_type ='business_customer'
          AND  s.submitted_date::DATE <= CURRENT_DATE AND 
          s.submitted_date::DATE >= DATE_TRUNC('MONTH', DATEADD('MONTH', -12, CURRENT_DATE))
)
,declined AS (
    SELECT 
        o.order_id,
        q.event_timestamp ,
        q.order_number,
        q.decision,
        q.decision_message ,
        TO_TIMESTAMP(q.event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as decision_date,
        CASE WHEN q.decision = 'decline' THEN q.decision_message 
             ELSE NULL 
        END AS declined_reason,
        CASE WHEN q.decision = 'decline' THEN decision_date 
        END AS declined_date
    FROM orders o
        LEFT JOIN stg_kafka_events_full.stream_internal_risk_order_decisions_v3 q 
            ON q.order_number = o.order_id
    WHERE declined_date IS NOT NULL        
    QUALIFY ROW_NUMBER () OVER (PARTITION BY order_number ORDER BY event_timestamp DESC) = 1        
)
, delivery_status AS (
    SELECT  
        o.subscription_id, 
        o.order_id,
        a2.shipment_at, 
        a2.delivered_at
    FROM  orders o
        LEFT JOIN master.allocation a2 
            ON o.subscription_id = a2.subscription_id
    WHERE a2.rank_allocations_per_subscription >= 1
    QUALIFY ROW_NUMBER()Over(PARTITION BY o.subscription_id ORDER BY a2.allocated_at DESC)=1   
)
SELECT 
    s.subscription_id,
    s.order_id,
    s.customer_id,
    s.account_name, 
    s.order_value,
    s.subscription_value,
    s.new_recurring,
    s.submitted_date::DATE,
    s.approved_date::DATE,
    d.declined_date::DATE,
    s.days_since_submitted_approved,
    s.approval_time,
    ds.shipment_at::DATE AS shipment_date,
    DATEDIFF('day',s.submitted_date::DATE, COALESCE(ds.shipment_at::DATE,CURRENT_DATE)) AS days_since_submitted_shipped,
    CASE WHEN s.status IN('DECLINED') AND ds.shipment_at IS NULL THEN s.status
         WHEN days_since_submitted_shipped > 0 AND days_since_submitted_shipped <= 7 THEN 'within 7 days'
         WHEN days_since_submitted_shipped > 7 AND days_since_submitted_shipped <= 15 THEN 'within 15 days'
         WHEN days_since_submitted_shipped > 15 AND days_since_submitted_shipped <= 30 THEN 'within 30 days'
         WHEN days_since_submitted_shipped > 30 THEN 'More than 30 days'
    END AS shipping_time,
    DATEDIFF('day',s.submitted_date::DATE, COALESCE (ds.delivered_at::DATE,CURRENT_DATE)) AS days_since_submitted_del,
    ds.delivered_at::DATE AS delivered_date,
    af.order_journey_mapping_risk AS risk_status,
    s.company_status,
    s.status,
    s.full_name AS account_owner,
    s.segment
FROM orders s
    LEFT JOIN declined d
        ON s.order_id = d.order_id
    LEFT JOIN delivery_status ds 
        ON ds.subscription_id = s.subscription_id
    LEFT JOIN dm_risk.approval_funnel af 
        ON af.order_id = s.order_id        
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_b2b.v_b2b_orders_overview TO tableau;