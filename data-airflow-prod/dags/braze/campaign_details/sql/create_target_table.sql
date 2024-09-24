CREATE TABLE IF NOT EXISTS stg_external_apis.braze_campaign_details
(
	row_id VARCHAR(32),
	campaign_id VARCHAR(100),
	campaign_name VARCHAR(100),
	created_at VARCHAR(50),
	updated_at VARCHAR(50),
	description VARCHAR(1000),
	channels VARCHAR(100),
	tags VARCHAR(1000),
	message_variation_id VARCHAR(100),
	message_variation_channel VARCHAR(100),
	message_variation_name VARCHAR(50),
	extracted_at VARCHAR(50)
)
DISTSTYLE KEY
 DISTKEY(row_id)
 SORTKEY(row_id);

GRANT SELECT ON stg_external_apis.braze_campaign_details TO redash_growth;
