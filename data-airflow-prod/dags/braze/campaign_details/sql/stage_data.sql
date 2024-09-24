DROP TABLE IF EXISTS stg_external_apis_dl.braze_campaign_details;

CREATE TABLE stg_external_apis_dl.braze_campaign_details
    DISTKEY(row_id)
    SORTKEY(row_id)
AS SELECT
    md5(campaign_id || message_variation_id)::VARCHAR(32) as row_id,
    campaign_id::VARCHAR(100),
    campaign_name::VARCHAR(100),
    created_at::VARCHAR(50),
    updated_at::VARCHAR(50),
    description::VARCHAR(1000),
    channels::VARCHAR(100),
    tags::VARCHAR(1000),
    message_variation_id::VARCHAR(100),
    message_variation_channel::VARCHAR(100),
    NULLIF(message_variation_name, 'NULL')::VARCHAR(50),
    extracted_at::VARCHAR(50)
FROM s3_spectrum_braze.braze_campaign_details_curated
    WHERE year = '{{ti.xcom_pull(key='year')}}'
    AND month = '{{ti.xcom_pull(key='month')}}'
    AND day = '{{ti.xcom_pull(key='day')}}'
    AND batch_id = '{{ti.xcom_pull(key='batch_id_value')}}';
