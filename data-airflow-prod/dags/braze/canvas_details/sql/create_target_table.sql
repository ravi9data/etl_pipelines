CREATE TABLE IF NOT EXISTS stg_external_apis.braze_canvas_details
(
	row_id VARCHAR(32),
	canvas_id VARCHAR(100),
	canvas_name VARCHAR(100),
	created_at VARCHAR(50),
	updated_at VARCHAR(50),
	description VARCHAR(1000),
	archived BOOLEAN,
	draft BOOLEAN,
	schedule_type VARCHAR(50),
	first_entry VARCHAR(50),
	last_entry VARCHAR(50),
	channels VARCHAR(100),
	variants VARCHAR(2000),
	tags VARCHAR(1000),
	steps_name VARCHAR(100),
	steps_id VARCHAR(100),
	next_step_ids VARCHAR(100),
	steps_channels VARCHAR(100),
	extracted_at VARCHAR(50)
)
DISTSTYLE KEY
DISTKEY(row_id)
SORTKEY(row_id);

GRANT SELECT ON stg_external_apis.braze_canvas_details TO redash_growth;
