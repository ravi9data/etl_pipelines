BEGIN;

DELETE FROM {{ var.json.pricing_availability_ideal_state_config.redshift_target_schema }}.{{ var.json.pricing_availability_ideal_state_config.redshift_target_table }};

COPY {{ var.json.pricing_availability_ideal_state_config.redshift_target_schema }}.{{ var.json.pricing_availability_ideal_state_config.redshift_target_table }}
FROM '{{ti.xcom_pull(key='s3_outfile')}}'
IAM_ROLE '{{var.value.redshift_iam_role}}'
FORMAT AS PARQUET;

COMMIT;
