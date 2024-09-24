DROP VIEW IF EXISTS marketing.v_partnership_order_validation;
CREATE VIEW marketing.v_partnership_order_validation AS
WITH everflow_campaigns AS (
    SELECT DISTINCT
        a.session_id,
        a.marketing_source,
        a.marketing_campaign,
        b.order_id,
        ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY a.page_view_start DESC) AS rn
    FROM traffic.page_views a
             LEFT JOIN traffic.session_order_mapping b ON a.session_id = b.session_id
    WHERE a.page_view_start::DATE >= '2023-09-01' AND LOWER(a.marketing_medium) IN (
                                                                                    'branding_partnerships',
                                                                                    'add-on_partnerships',
                                                                                    'edu_partnerships',
                                                                                    'mno_integration',
                                                                                    'strategic_partnerships',
                                                                                    'growth_partnerships')
)

   ,base_data AS (
    SELECT DISTINCT
        a.click_time AS click_date
                  ,a.country AS affiliate_country
                  ,a.order_id
                  ,b.marketing_source AS affiliate
                  ,'Everflow'::TEXT AS affiliate_network
                  ,b.marketing_campaign
    FROM marketing.partnership_everflow_submitted_orders a
             LEFT JOIN everflow_campaigns b ON a.order_id = b.order_id AND b.rn = 1

    UNION ALL

    SELECT DISTINCT
        o.submitted_date AS click_date,
        o.store_country AS affiliate_country,
        o.order_id,
                LOWER(COALESCE(pv.partnership_group,
                       (CASE WHEN o.store_id = 631
                                 THEN 'bob.at'
                           END),
                       CASE WHEN m.marketing_source = 'n/a' THEN COALESCE(mm.partnership_group_name, m.marketing_campaign)
                            ELSE COALESCE(mm.partnership_group_name,m.marketing_source) END
            )) AS affiliate,
        'Partnership'::TEXT AS affiliate_network,   --we called it Partnership in commission mapping file
        o.marketing_campaign
    FROM master.order o
             LEFT JOIN stg_external_apis.partnerships_vouchers pv
                       ON o.voucher_code 
                           ILIKE voucher_prefix_code+'%'
             LEFT JOIN ods_production.order_marketing_channel m
                       ON o.order_id = m.order_id
             LEFT JOIN staging.partnership_names_mapping mm
                       ON mm.partnership_name_original = (CASE WHEN m.marketing_source = 'n/a' THEN m.marketing_campaign
                                                               ELSE m.marketing_source END)
                           AND mm.b2c_b2b = (CASE WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END)
                           AND mm.country = o.store_country
             LEFT JOIN marketing.partnership_everflow_submitted_orders eo ON eo.order_id = o.order_id
    WHERE o.submitted_date >= '2023-09-01' 
        AND eo.order_id IS NULL
        AND (o.marketing_channel = 'Partnerships' OR o.store_id = 631)
        AND LOWER(affiliate) IN (SELECT DISTINCT LOWER(affiliate) FROM staging_google_sheet.partnerships_commission_mapping WHERE affiliate_network = 'Partnership')
)

   ,commission_validation AS (
    SELECT
        *
         ,ROW_NUMBER() OVER (PARTITION BY order_id, click_date IS NULL ORDER BY click_date DESC) AS rn
    FROM base_data
)

   ,exclusion_list AS (
    SELECT DISTINCT order_id
    FROM marketing.partnership_validated_orders
    WHERE COALESCE(submitted_date::DATE,'9999-01-01') < DATE_ADD('day', -15, current_date) 
),

    delivery_costs AS (
    select
        order_id,
        currency,
        sum(amount_paid) as delivery_cost
    from master.asset_payment
    where payment_type = 'SHIPMENT'
    group by 1,2
)

SELECT DISTINCT
    c.affiliate_network
    ,c.affiliate
    ,c.marketing_campaign
    ,c.order_id
    ,o.new_recurring
    ,o.status AS order_status
    ,c.affiliate_country
    ,o.store_country
    ,o.customer_type
    ,CASE WHEN o.voucher_value::float <= 40 THEN TRUE ELSE FALSE END is_recuring_voucher
    ,o.created_date
    ,o.order_value
    ,o.basket_size
    ,o.voucher_discount
    ,o.voucher_code
    ,dc.delivery_cost
    ,o.submitted_date
    ,CASE
         WHEN o.store_country = 'United States'
             THEN 'USD'
         WHEN o.store_country IN ('Germany', 'Austria', 'Spain', 'Netherlands')
             THEN 'EUR'
    END currency
    ,COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) AS exchange_rate
    ,CASE
         WHEN o.status = 'PAID' AND c.rn = 1
             THEN 'APPROVED'
         ELSE 'DECLINED'
    END commission_approval
     ma.commission_type AS _commission_type
    ,ma.commission_amount::DOUBLE PRECISION AS _commission_amount
    ,ma.affiliate_network_fee_rate::DOUBLE PRECISION AS _affiliate_network_fee_rate
    ,ROUND(CASE
               WHEN _commission_type = 'ABSOLUT'
                   THEN _commission_amount
               WHEN _commission_type = 'PERCENTAGE' AND LOWER(c.affiliate) = 'vivid'
                   THEN (o.basket_size + COALESCE(dc.delivery_cost,0)) * _commission_amount /100
               WHEN _commission_type = 'PERCENTAGE'
FROM commission_validation c
  LEFT JOIN exclusion_list el
            ON c.order_id = el.order_id
  LEFT JOIN master.ORDER o
            ON c.order_id = o.order_id
  LEFT JOIN staging_google_sheet.partnerships_commission_mapping ma
            ON LOWER(c.affiliate) = LOWER(REPLACE(ma.affiliate,' ',''))
                AND LOWER(c.affiliate_network) = LOWER(ma.affiliate_network)
                AND o.new_recurring = ma.new_recurring
                AND o.store_country = ma.country
                AND o.submitted_date::DATE BETWEEN ma.valid_from AND ma.valid_until
                AND CASE WHEN o.submitted_date::DATE >= '2024-08-01' THEN 
                    CASE WHEN c.campaign_name IS NOT NULL THEN c.marketing_campaign = lower(ma.campaign_name) ELSE TRUE END 
                    ELSE TRUE END 
  LEFT JOIN trans_dev.daily_exchange_rate exc
            ON o.submitted_date::DATE = exc.date_
                AND CASE
                        WHEN o.store_country = 'United States'
                            THEN 'USD'
                        END = exc.currency
  LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
            ON CASE
                   WHEN o.store_country = 'United States'
                       THEN 'USD'
                   END = exc.currency
  LEFT JOIN delivery_costs dc ON o.order_id = dc.order_id
WHERE TRUE
  AND el.order_id IS NULL
WITH NO SCHEMA BINDING;

GRANT SELECT ON marketing.v_partnership_order_validation TO tableau;
