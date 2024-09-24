DROP TABLE IF EXISTS web.session_order_mapping_snowplow;
CREATE TABLE web.session_order_mapping_snowplow AS 
WITH base AS (
SELECT DISTINCT 
 'event' AS src
 ,session_id
 ,order_id
FROM scratch.session_order_event_mapping
  
UNION ALL 

SELECT DISTINCT 
 'url' AS src
 ,session_id
 ,order_id
FROM scratch.session_order_url_mapping

UNION ALL 
 
SELECT DISTINCT 
 'customer' AS src
 ,session_id
 ,order_id
FROM scratch.session_order_sp_user_mapping
WHERE order_id IS NOT NULL

UNION ALL 

SELECT DISTINCT
 'snowplow_user' AS src
 ,session_id
 ,order_id
FROM scratch.session_order_snowplow_user_mapping
WHERE order_id IS NOT NULL
)
,base_agg AS (
SELECT DISTINCT
 a.session_id
 ,a.order_id
 ,LISTAGG(a.src,' / ') WITHIN GROUP (ORDER BY a.src)  AS list_of_sources
FROM base a
GROUP BY 1,2
)
,combined AS (
SELECT DISTINCT 
  agg.list_of_sources
 ,agg.session_id
 ,agg.order_id
 ,s.snowplow_user_id AS customer_id_snowplow
 ,s.encoded_customer_id
 ,s.customer_id AS customer_id_web
 ,s.marketing_channel
 ,s.marketing_source
 ,s.marketing_medium
 ,mo.customer_id AS customer_id_order
 ,c.created_at AS signup_date
 ,s.session_index
 ,s.session_start      
 ,CASE 
  WHEN s.session_start < COALESCE(mo.submitted_date, mo.created_date)::DATE-30 
   THEN NULL 
  ELSE s.session_start 
 END AS session_start_30d
 ,CASE 
   WHEN s.marketing_channel IN ('Direct', 'Other') 
    THEN NULL 
   ELSE s.session_start 
  END AS session_start_excl_direct
 ,s.session_end
 ,s.os
 ,s.browser
 ,s.geo_city
 ,s.device_type
 ,mo.created_date AS cart_date
 ,mo.address_orders
 ,mo.payment_orders
 ,mo.submitted_date
 ,mo.paid_date
 ,mo.voucher_code
 ,mo.new_recurring
 ,RANK() OVER (PARTITION BY agg.order_id ORDER BY s.session_start, s.session_id) AS session_rank_order
 ,CASE 
   WHEN session_start_30d IS NOT NULL 
    THEN RANK() OVER (PARTITION BY agg.order_id ORDER BY session_start_30d, s.session_id) 
  END AS session_rank_order_30d
 ,CASE 
   WHEN session_start_excl_direct IS NOT NULL 
    THEN RANK() OVER (PARTITION BY agg.order_id ORDER BY session_start_excl_direct, s.session_id) 
  END AS session_rank_order_excl_direct
 ,RANK() OVER (PARTITION BY agg.order_id, s.marketing_channel ORDER BY s.session_start, s.session_id) AS session_rank_order_channel
 ,COUNT(agg.session_id) OVER (PARTITION BY agg.order_id) AS session_count_order
 ,COUNT(CASE 
   WHEN s.marketing_channel NOT IN ('Direct', 'Other') 
    THEN agg.session_id 
   END) OVER (PARTITION BY agg.order_id) AS session_count_excl_direct_order
 ,CASE 
   WHEN session_rank_order = 1 
    THEN TRUE 
   ELSE FALSE 
  END AS first_touchpoint
 ,CASE 
   WHEN session_rank_order_30d = 1 
    THEN TRUE 
   ELSE FALSE 
  END AS first_touchpoint_30d 
 ,MAX(CASE 
   WHEN mo.created_date BETWEEN s.session_start AND s.session_end 
    THEN s.session_start 
  END) OVER (PARTITION BY agg.order_id) AS last_touchpoint_before_cart
  ,CASE 
   WHEN mo.created_date IS NOT NULL AND last_touchpoint_before_cart IS null
    THEN MAX(CASE 
   WHEN mo.created_date > s.session_start
    THEN s.session_start
   END) OVER (PARTITION BY agg.order_id) END AS last_touchpoint_before_cart2 
 ,MAX(CASE 
   WHEN mo.submitted_date BETWEEN s.session_start AND s.session_end 
    THEN s.session_start
   END) OVER (PARTITION BY agg.order_id) AS last_touchpoint_before_submitted
   ,CASE 
   WHEN mo.submitted_date IS NOT NULL AND last_touchpoint_before_submitted IS null
    THEN MAX(CASE 
   WHEN mo.submitted_date > s.session_start
    THEN s.session_start
   END) OVER (PARTITION BY agg.order_id) END AS last_touchpoint_before_submitted2,
   ROW_NUMBER() OVER(PARTITION BY agg.order_id, s.session_start ORDER BY s.session_index) AS rn
FROM base_agg agg
  LEFT JOIN web.sessions_snowplow s 
    ON agg.session_id = s.session_id
  INNER JOIN master.order mo 
    ON agg.order_id = mo.order_id  
      AND 
        CASE WHEN s.session_start::DATE <= '2022-06-21' 
          THEN DATE_DIFF('day', s.session_start::DATE, mo.created_date::DATE) <= 90 
        ELSE DATE_DIFF('day', s.session_start::DATE, mo.created_date::DATE) <= 30 
        END
  LEFT JOIN master.customer c 
    ON mo.customer_id = c.customer_id 
WHERE session_start::DATE >= '2019-01-29' 
 AND (mo.submitted_date IS NULL OR mo.submitted_Date >= s.session_start)
)
SELECT 
  list_of_sources
 ,session_id
 ,order_id
 ,customer_id_snowplow
 ,encoded_customer_id
 ,customer_id_web
 ,marketing_channel
 ,marketing_source
 ,marketing_medium
 ,os
 ,browser
 ,geo_city
 ,device_type
 ,customer_id_order
 ,signup_date
 ,session_index
 ,session_start
 ,session_end
 ,cart_date
 ,address_orders
 ,payment_orders
 ,submitted_date
 ,paid_date
 ,voucher_code
 ,new_recurring
 ,session_rank_order
 ,session_rank_order_excl_direct
 ,session_rank_order_channel
 ,session_count_order
 ,session_count_excl_direct_order
 ,first_touchpoint
 ,first_touchpoint_30d
 ,CASE 
   WHEN cart_date IS NULL AND session_rank_order = session_count_order 
    THEN TRUE 
   WHEN COALESCE(last_touchpoint_before_cart2, last_touchpoint_before_cart) = session_start
    THEN TRUE 
   ELSE FALSE 
  END AS last_touchpoint_before_cart
 ,CASE 
   WHEN submitted_date IS NULL AND session_rank_order = session_count_order 
    THEN TRUE 
   WHEN COALESCE(last_touchpoint_before_submitted2, last_touchpoint_before_submitted) = session_start
    THEN TRUE 
   ELSE FALSE 
  END AS last_touchpoint_before_submitted
FROM combined 
WHERE rn = 1
;