BEGIN;

DELETE FROM stg_external_apis.braze_campaign_details
WHERE row_id IN (SELECT row_id FROM stg_external_apis_dl.braze_campaign_details);

INSERT INTO stg_external_apis.braze_campaign_details (
	row_id,
	campaign_id,
	campaign_name,
	created_at,
	updated_at,
	description,
	channels,
	tags,
	message_variation_id,
	message_variation_channel,
	message_variation_name,
	extracted_at
)
SELECT
	row_id,
	campaign_id,
	campaign_name,
	created_at,
	updated_at,
	description,
	channels,
	tags,
	message_variation_id,
	message_variation_channel,
	message_variation_name,
	extracted_at
FROM stg_external_apis_dl.braze_campaign_details;

COMMIT;
