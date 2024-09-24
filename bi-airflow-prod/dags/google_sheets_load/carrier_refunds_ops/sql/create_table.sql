CREATE TABLE IF NOT EXISTS staging_google_sheet.carrier_refunds_ops(
        description VARCHAR(60000),
        refund_amount VARCHAR,
        carrier VARCHAR,
        duplicates VARCHAR,
        fact_date VARCHAR
);