DROP TABLE IF EXISTS stg_external_apis_dl.braze_canvas_details;

CREATE TABLE stg_external_apis_dl.braze_canvas_details
    DISTKEY(row_id)
    SORTKEY(row_id)
AS SELECT
    md5(canvas_id || steps_id)::VARCHAR(32) as row_id,
    canvas_id::VARCHAR(100),
    canvas_name::VARCHAR(100),
    created_at::VARCHAR(50),
    updated_at::VARCHAR(50),
    description::VARCHAR(1000),
    archived::BOOLEAN,
    draft::BOOLEAN,
    schedule_type::VARCHAR(50),
    first_entry::VARCHAR(50),
    last_entry::VARCHAR(50),
    channels::VARCHAR(100),
    variants::VARCHAR(2000),
    tags::VARCHAR(1000),
    steps_name::VARCHAR(100),
    steps_id::VARCHAR(100),
    next_step_ids::VARCHAR(100),
    steps_channels::VARCHAR(100),
    extracted_at::VARCHAR(50)
FROM s3_spectrum_braze.braze_canvas_details_curated
    WHERE year = '{{ti.xcom_pull(key='year')}}'
    AND month = '{{ti.xcom_pull(key='month')}}'
    AND day = '{{ti.xcom_pull(key='day')}}'
    AND batch_id = '{{ti.xcom_pull(key='batch_id_value')}}';
