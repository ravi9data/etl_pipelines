
CREATE TEMP TABLE tmp_one_row_per_order AS
SELECT
    order_id,
    TO_TIMESTAMP(posting_date, 'MM/DD/YYYY HH:MI:SS AM') AS posting_date,
    TO_TIMESTAMP(event_date, 'MM/DD/YYYY HH:MI:SS AM') AS event_date,
    publisher_name,
    website_name,
    ROW_NUMBER() over (PARTITION BY order_id ORDER BY event_date ASC) AS rn
FROM staging.cj_orders;

INSERT INTO marketing.affiliate_cj_submitted_orders
SELECT
    a.order_id,
    a.posting_date,
    a.event_date,
    a.publisher_name,
    a.website_name
FROM tmp_one_row_per_order a
         LEFT JOIN marketing.affiliate_cj_submitted_orders b USING (order_id)
WHERE b.order_id IS NULL AND a.rn = 1;
