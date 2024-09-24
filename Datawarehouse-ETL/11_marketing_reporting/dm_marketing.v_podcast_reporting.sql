DROP VIEW IF EXISTS dm_marketing.v_podcast_reporting;
CREATE VIEW dm_marketing.v_podcast_reporting AS
WITH podcast_vouchers AS (
    SELECT DISTINCT CASE WHEN voucher IN ('BAYWATCH15',
            'APPLE15',
            'BAYWATCH100') THEN 'BAYWATCH' 
        ELSE voucher
        END AS voucher_code 
    FROM staging.podcasts_voucher_tracking),

podcast_sources AS (
    SELECT DISTINCT CASE WHEN voucher IN ('BAYWATCH15',
            'APPLE15',
            'BAYWATCH100') THEN 'BAYWATCH' 
        ELSE voucher 
        END AS voucher_code,
        LOWER(utm_source) AS utm_source 
    FROM staging.podcasts_voucher_tracking),
    
podcast_campaigns AS (
    SELECT DISTINCT CASE WHEN voucher IN ('BAYWATCH15',
            'APPLE15',
            'BAYWATCH100') THEN 'BAYWATCH' 
        ELSE voucher 
        END AS voucher_code,
        LOWER(utm_campaign) AS utm_campaign
    FROM staging.podcasts_voucher_tracking),

podcast_orders_prep AS (
    SELECT o.customer_id,
       o.order_id,
       o.status,
       o.new_recurring,
       o.marketing_channel,
       o.created_date::DATE AS created_date,
       o.paid_date::DATE AS paid_date,
       o.submitted_date::DATE AS submitted_date,
       CASE WHEN o.voucher_code IN ('BAYWATCH15',
            'APPLE15',
            'BAYWATCH100') THEN 'BAYWATCH' ELSE o.voucher_code END AS voucher_code, 
       o.completed_orders,
       o.paid_orders,
       o.order_value,
       s.subscription_id ,
       s.rental_period,
       s.effective_duration ,
       s.start_date ,
       s.cancellation_date ,
       s.subscription_value_euro,
       s.committed_sub_value,
       s.subscription_value_euro * s.effective_duration AS total_sub_value
    FROM master.ORDER o
        LEFT JOIN master.subscription s ON o.order_id = s.order_id
        LEFT JOIN podcast_vouchers pv ON pv.voucher_code = o.voucher_code
    WHERE o.voucher_code IN ('BAYWATCH15',
            'APPLE15',
            'BAYWATCH100') OR pv.voucher_code IS NOT NULL),

podcast_orders AS (
    SELECT voucher_code AS voucher,
       new_recurring,
       COUNT(DISTINCT customer_id) AS customer,
       COUNT(DISTINCT order_id) AS total_orders,
       COUNT(DISTINCT CASE WHEN completed_orders >=1 THEN order_id END) AS total_submitted,
       COUNT(DISTINCT CASE WHEN paid_orders >=1 THEN order_id END) AS total_paid,
       COUNT(DISTINCT subscription_id) AS total_subs,
       COUNT(DISTINCT CASE WHEN cancellation_date IS NULL AND paid_orders >=1 THEN subscription_id END) AS total_active_subs,
       COUNT(DISTINCT CASE WHEN cancellation_date IS NOT NULL THEN subscription_id END) AS total_canceled_subs,
       SUM(CASE WHEN cancellation_date IS NULL AND paid_orders >=1 THEN total_sub_value END) AS total_active_subs_value,
       SUM(CASE WHEN cancellation_date IS NOT NULL THEN total_sub_value END) AS total_canceled_subs_value,
       SUM(committed_sub_value) AS total_committed_sub_value,
       SUM(total_sub_value) AS total_subscription_value,
       AVG(rental_period) AS avg_rental_period,
       AVG(effective_duration) AS avg_effective_duration
    FROM podcast_orders_prep
    GROUP BY 1,2),

sessions AS (
    SELECT DISTINCT a.session_id,  
        COALESCE(b.voucher_code,c.voucher_code) AS voucher
    FROM traffic.page_views a
        LEFT JOIN podcast_sources b on a.marketing_source = b.utm_source 
        LEFT JOIN podcast_campaigns c on a.marketing_campaign = c.utm_campaign
    WHERE b.voucher_code IS NOT NULL OR c.voucher_code IS NOT NULL
),

podcast_traffic AS (
    SELECT
        CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
        ss.voucher,
        COUNT(DISTINCT s.session_id) AS sessions,
        COUNT(DISTINCT COALESCE(s.customer_id, s.anonymous_id)) AS unique_users,
        COUNT(DISTINCT CASE WHEN s.page_type = 'pdp' THEN s.session_id END) AS sessions_with_product_page_views
    FROM traffic.page_views s  --USING page views to get anytouch sessions AS we have small number of sessions generaly
        INNER JOIN sessions ss USING (session_id)
        LEFT JOIN traffic.snowplow_user_mapping cu ON s.anonymous_id = cu.anonymous_id
            AND s.session_id = cu.session_id
    GROUP BY 1,2),

podcast_costs AS (
    SELECT
        b.voucher_code AS voucher,
        'NEW' AS new_recurring,
        SUM(a.total_spent_local_currency) * 0.8 AS cost
    FROM marketing.marketing_cost_daily_podcasts a
        LEFT JOIN podcast_sources b on a.podcast_name = b.utm_source
    WHERE a.date IS NOT NULL AND date <= current_date
    GROUP BY 1,2

    UNION ALL

    SELECT
        b.voucher_code AS voucher,
        'RECURRING' AS new_recurring,
        SUM(total_spent_local_currency) *  0.2 AS cost
    FROM marketing.marketing_cost_daily_podcasts a
        LEFT JOIN podcast_sources b on a.podcast_name = b.utm_source
    WHERE a.date IS NOT NULL AND date <= current_date
    GROUP BY 1,2),

mapping_data AS (
    SELECT voucher_code AS voucher,
           'NEW' AS new_recurring
    FROM podcast_vouchers
    UNION ALL
    SELECT voucher_code AS voucher,
           'RECURRING' AS new_recurring
    FROM podcast_vouchers
    )

    SELECT m.*,
        a.customer,
        a.total_orders,
        a.total_submitted,
        a.total_paid,
        a.total_subs,
        a.total_active_subs,
        a.total_canceled_subs,
        a.total_active_subs_value,
        a.total_canceled_subs_value,
        a.total_committed_sub_value,
        a.total_subscription_value,
        a.avg_rental_period,
        a.avg_effective_duration,
        b.cost,
        c.sessions,
        c.unique_users,
        c.sessions_with_product_page_views
    FROM mapping_data m
        LEFT JOIN podcast_orders a on m.voucher = a.voucher AND m.new_recurring = a.new_recurring
        FULL JOIN podcast_costs b on COALESCE(m.new_recurring,a.new_recurring)=b.new_recurring
            AND COALESCE(m.voucher,a.voucher) = b.voucher
        FULL JOIn podcast_traffic c on COALESCE(m.new_recurring, a.new_recurring, b.new_recurring)=c.new_recurring
            AND COALESCE(m.voucher,a.voucher,b.voucher) = c.voucher
    WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_marketing.v_podcast_reporting to tableau;
