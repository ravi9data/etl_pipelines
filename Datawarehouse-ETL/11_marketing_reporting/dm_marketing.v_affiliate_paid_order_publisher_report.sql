DROP VIEW IF EXISTS dm_marketing.v_affiliate_paid_order_publisher_report;
CREATE VIEW dm_marketing.v_affiliate_paid_order_publisher_report AS
WITH
    all_affiliate_orders AS (
        SELECT
            submitted_date AS transaction_date,
            order_country AS country,
            order_id,
            CASE WHEN affiliate_network ilike '%tradedoubler%' then 'Tradedoubler' else affiliate_network end as affiliate_network,
            affiliate,
            'Automation'::varchar AS source,
            commission_approval::varchar as commission_approval
        from marketing.affiliate_validated_orders
        WHERE submitted_date::DATE >= '2023-01-01'

        UNION

        SELECT
            "date" AS transaction_date,
            country,
            order_id,
            CASE WHEN affiliate_network ilike '%tradedoubler%' then 'Tradedoubler' else affiliate_network end as affiliate_network,
            affiliate,
            'Manual Approval'::varchar AS source,
            'APPROVED MANUALLY'::varchar AS commission_approval
        FROM marketing.marketing_cost_daily_affiliate_order a
                 LEFT JOIN master.order b USING (order_id)
        WHERE "date" BETWEEN '2021-09-01' AND '2022-12-31'
    ),

    tmp_approved_orders AS (
        SELECT
            a.*,
            o.marketing_channel,
            o.new_recurring,
            o.customer_type ,
            s.currency,
            CASE WHEN o.paid_orders >= 1 then 'paid' else 'not_paid' end as order_type,
            COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) AS exchange_rate,
            COALESCE(ma.commission_type, ma2.commission_type) AS _commission_type,
            COALESCE(ma.commission_amount, ma2.commission_amount)::double precision AS _commission_amount,
            COALESCE(ma.affiliate_network_fee_rate, ma2.affiliate_network_fee_rate)::double precision AS _affiliate_network_fee_rate,
            ROUND(CASE
                      WHEN _commission_type = 'ABSOLUT'
                          THEN _commission_amount
                      WHEN _commission_type = 'PERCENTAGE'
        FROM all_affiliate_orders a
                 LEFT JOIN master.order o on o.order_id = a.order_id
                 LEFT JOIN master.subscription s ON a.order_id = s.order_id
                 LEFT JOIN staging_google_sheet.affliates_commission_mapping ma ON LOWER(a.affiliate) = LOWER(ma.affiliate)
            AND LOWER(a.affiliate_network) = LOWER(ma.affiliate_network)
            AND o.new_recurring = ma.new_recurring
            AND o.submitted_date::DATE BETWEEN ma.valid_from AND ma.valid_until
                 LEFT JOIN staging_google_sheet.affliates_commission_mapping ma2 ON ma.affiliate_network IS NULL
            AND ma2.affiliate IS NULL
            AND LOWER(a.affiliate_network) = LOWER(ma2.affiliate_network)
            AND o.new_recurring = ma2.new_recurring
            AND o.submitted_date::DATE BETWEEN ma2.valid_from AND ma2.valid_until
                 LEFT JOIN trans_dev.daily_exchange_rate exc ON o.submitted_date::DATE = exc.date_ AND
                                                                CASE
                                                                    WHEN o.store_country = 'United States'
                                                                        THEN 'USD'
                                                                    END = exc.currency
                 LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last ON
                CASE
                    WHEN o.store_country = 'United States'
                        THEN 'USD'
                    END = exc.currency
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, o.voucher_discount),

    tmp_affiliate_orders AS (
        SELECT DISTINCT
            order_id,
            session_id
        FROM tmp_approved_orders
                 INNER JOIN traffic.session_order_mapping USING (order_id)
    ),

    tmp_snowplow_users AS (
        SELECT DISTINCT
            anonymous_id,
            order_id
        FROM traffic.page_views
                 INNER JOIN tmp_affiliate_orders USING (session_id)
        WHERE anonymous_id IS NOT NULL
    ),

    tmp_customers AS (
        SELECT DISTINCT
            customer_id,
            order_id
        FROM traffic.page_views
                 INNER JOIN tmp_affiliate_orders USING (session_id)
        WHERE customer_id IS NOT NULL
    ),

    tmp_affiliate_interaction AS (
        SELECT DISTINCT a.order_id
        FROM tmp_affiliate_orders a
                 LEFT JOIN traffic.page_views b ON a.session_id = b.session_id
        WHERE (LOWER(COALESCE(NULLIF(b.marketing_medium, ''), b.referer_medium, b.marketing_network)) in
               ('affiliate', 'affiliates')
            OR referer_url LIKE '%srvtrck%'
            OR referer_url LIKE '%tradedoubler%' )
          AND b.page_view_start::DATE >= '2021-09-01'

        UNION

        SELECT DISTINCT a.order_id
        FROM tmp_snowplow_users a
                 LEFT JOIN traffic.page_views b ON a.anonymous_id = b.anonymous_id
        WHERE (LOWER(COALESCE(NULLIF(b.marketing_medium, ''), b.referer_medium, b.marketing_network)) in
               ('affiliate', 'affiliates')
            OR referer_url LIKE '%srvtrck%'
            OR referer_url LIKE '%tradedoubler%' )
          AND b.page_view_start::DATE >= '2021-09-01'

        UNION

        SELECT DISTINCT a.order_id
        FROM tmp_customers a
                 LEFT JOIN traffic.page_views b ON a.customer_id = b.customer_id
        WHERE (LOWER(COALESCE(NULLIF(b.marketing_medium, ''), b.referer_medium, b.marketing_network)) in
               ('affiliate', 'affiliates')
            OR referer_url LIKE '%srvtrck%'
            OR referer_url LIKE '%tradedoubler%')
          AND b.page_view_start::DATE >= '2021-09-01'
    ),

    final_approved AS (
        SELECT DISTINCT
            a.transaction_date,
            a.country,
            a.currency,
            a.order_id,
            a.customer_type,
            a.order_type,
            a.affiliate_network,
            a.affiliate,
            a.marketing_channel,
            a.new_recurring,
            a.source,
            a.commission_approval,
            CASE
                WHEN a.marketing_channel = 'Affiliates' then 'Last Touch'
                WHEN a.marketing_channel != 'Affiliates' AND b.order_id IS NOT NULL THEN 'Any Touch'
                WHEN a.marketing_channel != 'Affiliates' AND b.order_id iS NULL THEN 'Not Detected'
                WHEN a.order_id IS NULL then 'Not Send As Approved'
                END AS touchpoint
        FROM tmp_approved_orders a
                 LEFT JOIN tmp_affiliate_interaction b ON a.order_id = b.order_id
        WHERE a.order_id != 'R000000000'
    ),

    master_affiliate_last_touch AS (
        SELECT order_id,
               store_country AS country,
               a.paid_date::DATE AS transaction_date,
               CASE WHEN b.marketing_source = 'rakutenmarketing' THEN 'Rakuten'
                    WHEN b.marketing_source = 'everflow' THEN 'Everflow'
                    WHEN b.marketing_source = 'tradedoubler' THEN 'Tradedoubler'
                    WHEN b.marketing_source = 'daisycon' THEN 'Daisycon'
                    ELSE 'n/a'
                   END AS affiliate_network,
               a.marketing_campaign AS affiliate,
               CASE WHEN a.paid_orders >= 1 then 'paid' else 'not_paid' end as order_type,
               a.customer_type ,
               a.marketing_channel,
               a.new_recurring,
               'Master Order'::varchar AS source,
               'NO APPROVAL'::varchar AS commission_approval,
               'Last Touch Master'::text AS touchpoint
        FROM master."order" a
                 LEFT JOIN ods_production.order_marketing_channel b USING(order_id)
        WHERE a.marketing_channel = 'Affiliates' AND paid_orders >=1
    )

SELECT
    COALESCE(a.transaction_date, b.transaction_date) AS "date",
    COALESCE(a.country, b.country) AS country,
    a.total_spent_local_currency,
    a.currency,
    COALESCE(a.order_id, b.order_id) AS order_id,
    COALESCE(a.customer_type, b.customer_type) AS customer_type,
    COALESCE(a.order_type, b.order_type) AS order_type,
    COALESCE(a.source, b.source) AS source,
    COALESCE(a.commission_approval, b.commission_approval) AS commission_approval,
    COALESCE(a.affiliate_network, b.affiliate_network) AS affiliate_network,
    COALESCE(a.affiliate, b.affiliate) AS affiliate,
    COALESCE(a.marketing_channel, b.marketing_channel) AS marketing_channel,
    COALESCE(a.new_recurring, b.new_recurring) AS new_recurring,
    COALESCE(a.touchpoint, b.touchpoint) AS touchpoint
FROM final_approved a
   FULL OUTER JOIN master_affiliate_last_touch b USING(order_id)   
WITH NO SCHEMA BINDING;
GRANT SELECT ON TABLE dm_marketing.v_affiliate_paid_order_publisher_report TO matillion;
