DROP TABLE IF EXISTS staging.cj_orders;

CREATE TABLE staging.cj_orders
(
    posting_date VARCHAR(100),
    event_date VARCHAR(100),
    order_id VARCHAR(100),
    publisher_name VARCHAR(100),
    website_name VARCHAR(100)
);

COPY staging.cj_orders
    IAM_ROLE 'arn:aws:iam::031440329442:role/data-production-redshift-datawarehouse-s3-role'
    IGNOREHEADER AS 1
    DELIMITER ';'
    EMPTYASNULL
;
