DROP TABLE IF EXISTS traffic.session_order_mapping;
CREATE TABLE traffic.session_order_mapping AS
WITH base_web AS (
    SELECT DISTINCT
        'event-snowplow' AS src,
        session_id,
        order_id
    FROM scratch.session_order_event_mapping
    WHERE collector_tstamp < '2023-05-01'
      AND collector_tstamp >= '2019-01-29'

    UNION ALL

    SELECT DISTINCT
        'event-segment' AS src,
        session_id,
        order_id
    FROM segment.session_order_event_mapping_web
    WHERE event_time >= '2023-05-01'

    UNION ALL

    SELECT DISTINCT
        'customer-segment' AS src,
        session_id,
        order_id
    FROM segment.session_order_user_mapping_web
    WHERE session_start >= '2023-05-01'

    UNION ALL

    SELECT DISTINCT
        'url-snowplow' AS src,
        session_id,
        order_id
    FROM scratch.session_order_url_mapping
    WHERE etl_tstamp < '2023-05-01'
      AND etl_tstamp >= '2019-01-29'

    UNION ALL

    SELECT DISTINCT
        'customer-snowplow' AS src,
        session_id,
        order_id
    FROM scratch.session_order_sp_user_mapping
    WHERE order_id IS NOT NULL
      AND page_view_start < '2023-05-01'
      AND page_view_start >= '2019-01-29'

    UNION ALL

    SELECT DISTINCT
        'snowplow_user' AS src,
        session_id,
        order_id
    FROM scratch.session_order_snowplow_user_mapping
    WHERE order_id IS NOT NULL
      AND session_date < '2023-05-01'
      AND session_date >= '2019-01-29'
),

base_app AS (SELECT DISTINCT 'event-segment-app' AS src,
                             session_id,
                             order_id
             FROM segment.session_order_event_mapping_app
             WHERE event_time >= '2023-09-01'

             UNION ALL

             SELECT DISTINCT 'customer-segment-app' AS src,
                             session_id,
                             order_id
             FROM segment.session_order_user_mapping_app
             WHERE session_start >= '2023-09-01'),

base_agg_web AS (
         SELECT DISTINCT
             session_id,
             order_id,
             LISTAGG(src,' / ') WITHIN GROUP (ORDER BY src)  AS list_of_sources
         FROM base_web
         GROUP BY 1,2
     ),

base_agg_app AS (
         SELECT DISTINCT
             a.session_id,
             a.order_id,
             LISTAGG(a.src,' / ') WITHIN GROUP (ORDER BY a.src)  AS list_of_sources
         FROM base_app a
            LEFT JOIN base_agg_web b USING(session_id)
         WHERE b.session_id IS NULL
         GROUP BY 1,2
     ),
    
    base_agg AS (
        SELECT * FROM base_agg_app
        UNION ALL
        SELECT * FROM base_agg_web
    ),

     combined AS (
        SELECT DISTINCT
            ba.list_of_sources,
            ba.session_id,
            ba.order_id,
            t.anonymous_id,
            t.encoded_customer_id,
            t.customer_id AS customer_id_web,
            t.marketing_channel,
            t.marketing_source,
            t.marketing_medium,
            c.customer_id AS customer_id_order,
            d.created_at AS signup_date,
            t.session_index,
            t.session_start,
            CASE
                WHEN t.session_start < COALESCE(c.submitted_date, c.created_date)::DATE-30
                    THEN NULL
                ELSE t.session_start
                END AS session_start_30d,
            CASE
                WHEN t.marketing_channel IN ('Direct', 'Other')
                    THEN NULL
                ELSE t.session_start
                END AS session_start_excl_direct,
            t.session_end,
            t.os,
            NULL AS browser,
            NULL AS geo_city,
            t.device_type,
            c.created_date AS cart_date,
            c.address_orders,
            c.payment_orders,
            c.submitted_date,
            c.paid_date,
            c.voucher_code,
            c.new_recurring,
            RANK() OVER (PARTITION BY ba.order_id ORDER BY t.session_start, t.session_id) AS session_rank_order,
            CASE
                WHEN session_start_30d IS NOT NULL
                    THEN RANK() OVER (PARTITION BY ba.order_id ORDER BY session_start_30d, t.session_id)
                END AS session_rank_order_30d,
            CASE
                WHEN session_start_excl_direct IS NOT NULL
                    THEN RANK() OVER (PARTITION BY ba.order_id ORDER BY session_start_excl_direct, t.session_id)
                END AS session_rank_order_excl_direct,
            RANK() OVER (PARTITION BY ba.order_id, t.marketing_channel ORDER BY t.session_start, t.session_id) AS session_rank_order_channel,
            COUNT(ba.session_id) OVER (PARTITION BY ba.order_id) AS session_count_order,
            COUNT(CASE WHEN t.marketing_channel NOT IN ('Direct', 'Other') THEN ba.session_id END) OVER (PARTITION BY ba.order_id) AS session_count_excl_direct_order,
            CASE WHEN session_rank_order = 1
                     THEN TRUE
                 ELSE FALSE
                END AS first_touchpoint,
            CASE
                WHEN session_rank_order_30d = 1
                    THEN TRUE
                ELSE FALSE
                END AS first_touchpoint_30d ,
            MAX(CASE WHEN c.created_date BETWEEN t.session_start AND t.session_end
                         THEN t.session_start
                END) OVER (PARTITION BY ba.order_id) AS last_touchpoint_before_cart,
            CASE
                WHEN c.created_date IS NOT NULL AND last_touchpoint_before_cart IS NULL
                    THEN MAX(CASE
                                 WHEN c.created_date > t.session_start
                                     THEN t.session_start
                    END) OVER (PARTITION BY ba.order_id) END AS last_touchpoint_before_cart2 ,
            MAX(CASE WHEN c.submitted_date BETWEEN t.session_start AND t.session_end
                         THEN t.session_start
                END) OVER (PARTITION BY ba.order_id) AS last_touchpoint_before_submitted,
            CASE WHEN c.submitted_date IS NOT NULL AND last_touchpoint_before_submitted IS NULL
                     THEN MAX(CASE WHEN c.submitted_date > t.session_start
                                       THEN t.session_start
                    END) OVER (PARTITION BY ba.order_id) END AS last_touchpoint_before_submitted2,
            ROW_NUMBER() OVER (PARTITION BY ba.order_id, t.session_start ORDER BY t.session_index) AS rn
        FROM base_agg ba
            LEFT JOIN traffic.sessions t
                    ON ba.session_id = t.session_id 
            LEFT JOIN master.order c ON ba.order_id = c.order_id
            LEFT JOIN master.customer d ON c.customer_id = d.customer_id
        WHERE (c.submitted_date IS NULL OR c.submitted_date >= t.session_start)
     )

SELECT
    list_of_sources,
    session_id,
    order_id,
    anonymous_id,
    encoded_customer_id,
    customer_id_web,
    marketing_channel,
    marketing_source,
    marketing_medium,
    os,
    browser,
    geo_city,
    device_type,
    customer_id_order,
    signup_date,
    session_index,
    session_start,
    session_end,
    cart_date,
    address_orders,
    payment_orders,
    submitted_date,
    paid_date,
    voucher_code,
    new_recurring,
    session_rank_order,
    session_rank_order_excl_direct,
    session_rank_order_channel,
    session_count_order,
    session_count_excl_direct_order,
    first_touchpoint,
    first_touchpoint_30d,
    CASE
        WHEN cart_date IS NULL AND session_rank_order = session_count_order
            THEN TRUE
        WHEN COALESCE(last_touchpoint_before_cart2, last_touchpoint_before_cart) = session_start
            THEN TRUE
        ELSE FALSE
        END AS last_touchpoint_before_cart,
    CASE
        WHEN submitted_date IS NULL AND session_rank_order = session_count_order
            THEN TRUE
        WHEN COALESCE(last_touchpoint_before_submitted2, last_touchpoint_before_submitted) = session_start
            THEN TRUE
        ELSE FALSE
        END AS last_touchpoint_before_submitted
FROM combined
WHERE rn = 1;

GRANT SELECT ON traffic.session_order_mapping TO tableau;
GRANT SELECT ON traffic.session_order_mapping TO redash_growth;
