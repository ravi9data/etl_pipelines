
INSERT INTO marketing.affiliate_everflow_submitted_orders
SELECT
    a.conversion_id,
    timestamptz 'epoch' + a.conversion_unix_timestamp::int * interval '1 second' AS conversion_time,
    a.sub1,
    a.status,
    a.revenue::double precision,
    a.country,
    a.device_type,
    a.event,
    a.transaction_id,
    timestamptz 'epoch' + a.click_unix_timestamp::int * interval '1 second' AS click_time,
    a.sale_amount::double precision,
    a.coupon_code,
    a.url,
    a.order_id,
    a.currency_id,
    CURRENT_DATE AS loaded_at
FROM staging.everflow a
         LEFT JOIN marketing.affiliate_everflow_submitted_orders b USING (order_id)
WHERE b.order_id IS NULL;
