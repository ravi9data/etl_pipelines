BEGIN;

DELETE FROM stg_external_apis.braze_canvas_details
WHERE row_id IN (SELECT row_id FROM stg_external_apis_dl.braze_canvas_details);

INSERT INTO stg_external_apis.braze_canvas_details (
	row_id,
	canvas_id,
	canvas_name,
	created_at,
	updated_at,
	description,
	archived,
	draft,
	schedule_type,
	first_entry,
	last_entry,
	channels,
	variants,
	tags,
	steps_name,
	steps_id,
	next_step_ids,
	steps_channels,
	extracted_at
)
SELECT
	row_id,
	canvas_id,
	canvas_name,
	created_at,
	updated_at,
	description,
	archived,
	draft,
	schedule_type,
	first_entry,
	last_entry,
	channels,
	variants,
	tags,
	steps_name,
	steps_id,
	next_step_ids,
	steps_channels,
	extracted_at
FROM stg_external_apis_dl.braze_canvas_details;

COMMIT;
