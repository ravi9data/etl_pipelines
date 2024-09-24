DROP TABLE IF EXISTS staging.everflow;

CREATE TABLE staging.everflow
(
    conversion_id VARCHAR(100),
    conversion_unix_timestamp VARCHAR(100),
    sub1 VARCHAR(100),
    status VARCHAR(100),
    revenue VARCHAR(100),
    country VARCHAR(100),
    device_type VARCHAR(100),
    event VARCHAR(100),
    transaction_id VARCHAR(100),
    click_unix_timestamp VARCHAR(100),
    sale_amount VARCHAR(100),
    coupon_code VARCHAR(100),
    order_id VARCHAR(100),
    url VARCHAR(10000),
    currency_id VARCHAR(100)
);

COPY staging.everflow
    IAM_ROLE 'arn:aws:iam::031440329442:role/data-production-redshift-datawarehouse-s3-role'
    IGNOREHEADER AS 1
    DELIMITER ';'
    EMPTYASNULL
;