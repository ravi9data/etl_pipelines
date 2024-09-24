
CREATE TEMP TABLE tmp_partnership_everflow_order AS
SELECT
    order_id,
    country,
    timestamptz 'epoch' + conversion_unix_timestamp::int * interval '1 second' AS conversion_time,
    timestamptz 'epoch' + click_unix_timestamp::int * interval '1 second' AS click_time
FROM staging.partnership_everflow;

INSERT INTO marketing.partnership_everflow_submitted_orders
SELECT
    a.order_id,
    a.country,
    a.conversion_time,
    a.click_time
FROM tmp_partnership_everflow_order a
    LEFT JOIN marketing.partnership_everflow_submitted_orders b USING (order_id)
WHERE b.order_id IS NULL;
