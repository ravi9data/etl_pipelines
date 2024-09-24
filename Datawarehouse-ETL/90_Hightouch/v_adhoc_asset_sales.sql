CREATE OR REPLACE VIEW hightouch_sources.v_adhoc_asset_sales AS
WITH base_delivered AS (
    SELECT
        b.external_user_id as customer_id,
        b.canvas_name, --same customer might appear in multiple campaings, so define with Biagio how he wants it
        timestamp 'epoch' + b."time" * interval '1 second' AS delivered_time
    FROM stg_external_apis.braze_delivery_event b
    WHERE b.canvas_name ILIKE '%b2c_ad_hoc_test_asset_sale%'
),

    base_sent AS (
    SELECT
        b.external_user_id as customer_id,
        b.canvas_name, --same customer might appear in multiple campaings, so define with Biagio how he wants it
        timestamp 'epoch' + b."time" * interval '1 second' AS sent_time
    FROM stg_external_apis.braze_sent_event b
    WHERE b.canvas_name ILIKE '%b2c_ad_hoc_test_asset_sale%'
),

     base_open AS (
         SELECT
             b.external_user_id as customer_id,
             b.canvas_name,
             timestamp 'epoch' + b."time" * interval '1 second' AS open_time
         FROM stg_external_apis.braze_open_event b
         WHERE b.canvas_name ILIKE '%b2c_ad_hoc_test_asset_sale%'
     ),

     base_click AS (
         SELECT
             b.external_user_id as customer_id,
             b.canvas_name,
             timestamp 'epoch' + b."time" * interval '1 second' AS click_time
         FROM stg_external_apis.braze_click_event b
         WHERE b.canvas_name ILIKE '%b2c_ad_hoc_test_asset_sale%' and url ilike '%g-explore/one-time-purchase%'
     ),

    campaign_data AS (
        select customer_id,
               canvas_name,
               case when d.delivered_time is not null then TRUE else FALSE END as is_delivered,
               min(sent_time) as sent_date,
               min(open_time) as first_time_opened,
               min(click_time) as first_time_clicked
        from base_sent a
        left join base_open b using (customer_id,canvas_name)
        left join base_click c using (customer_id,canvas_name)
        left join base_delivered d using(customer_id,canvas_name)
        group by 1,2,3
    ),

subscription_data_added AS (
    SELECT
    b.customer_id,
    s.subscription_id,
    a2.asset_id,
    a2.product_sku,
    b.canvas_name, --same customer might appear in multiple campaings, so define with Biagio how he wants it
    a2.asset_status_original,
    a2.sold_date,
    a2.sold_price,
    b.sent_date,
    b.is_delivered,
    b.first_time_opened,
    b.first_time_clicked,
    row_number() over (partition by b.customer_id, s.subscription_id order by a.created_at desc) as rn
    FROM campaign_data b
    LEFT JOIN master.subscription s
    ON b.customer_id = s.customer_id
    LEFT JOIN master.allocation a
    ON a.subscription_id = s.subscription_id
    LEFT JOIN master.asset a2
    ON a2.asset_id = a.asset_id)

SELECT
    b.customer_id,
    b.subscription_id,
    b.asset_id,
    b.product_sku,
    b.canvas_name,
    b.sent_date,
    b.first_time_opened,
    b.first_time_clicked,
    b.is_delivered,
    b.sold_date,
    b.sold_price,
    b.asset_status_original
FROM subscription_data_added b
inner JOIN dm_commercial.subscription_list_ad_hoc_sale s
         ON s.subscription_id = b.subscription_id
where b.rn = 1
ORDER BY 6,1
WITH NO SCHEMA BINDING;

GRANT SELECT ON hightouch_sources.v_adhoc_asset_sales TO hightouch;
