
DROP TABLE IF EXISTS tmp_canvas_names;
CREATE TEMP TABLE tmp_canvas_names
SORTKEY(canvas_id)
DISTKEY(canvas_id)
AS
WITH canvas_names AS (
	SELECT DISTINCT 
		canvas_id, 
		canvas_name, 
		ROW_NUMBER() OVER(PARTITION BY canvas_id ORDER BY updated_at desc) AS row_no
	FROM stg_external_apis.braze_canvas_details
	WHERE canvas_id IS NOT NULL
)
SELECT 
	canvas_id, 
	canvas_name
FROM canvas_names
WHERE row_no = 1
;


DROP TABLE IF EXISTS tmp_canvas_step_names;
CREATE TEMP TABLE tmp_canvas_step_names
SORTKEY(canvas_step_id)
DISTKEY(canvas_step_id)
AS
WITH canvas_step_names AS (
	SELECT DISTINCT 
		steps_id AS canvas_step_id, 
		steps_name AS canvas_step_name, 
		ROW_NUMBER() OVER(PARTITION BY steps_id ORDER BY updated_at desc) AS row_no
	FROM stg_external_apis.braze_canvas_details
	WHERE steps_id IS NOT NULL
)
SELECT 
	canvas_step_id, 
	canvas_step_name
FROM canvas_step_names
WHERE row_no = 1
;



DROP TABLE IF EXISTS tmp_canvas_variation_names;
CREATE TEMP TABLE tmp_canvas_variation_names
SORTKEY(canvas_variation_id)
DISTKEY(canvas_variation_id)
AS
WITH canvas_step_names AS (
	SELECT DISTINCT 
		canvas_variation_id, 
		canvas_variation_name,
		ROW_NUMBER() OVER(PARTITION BY canvas_variation_id ORDER BY date desc) AS row_no
	FROM stg_external_apis.braze_canvas_entry_event bcee
	WHERE canvas_variation_id IS NOT NULL
)
SELECT 
	canvas_variation_id, 
	canvas_variation_name
FROM canvas_step_names
WHERE row_no = 1
;



BEGIN TRANSACTION;

DELETE FROM dm_marketing.braze_canvas_events_all
WHERE braze_canvas_events_all.sent_date::date >= DATEADD('day', -15, current_date);

INSERT INTO dm_marketing.braze_canvas_events_all
SELECT 
	cn.canvas_name,
	csn.canvas_step_name,
	cvn.canvas_variation_name,
	b.* 
FROM dm_marketing.braze_order_attribution b
LEFT JOIN tmp_canvas_variation_names cvn 
	ON b.canvas_variation_id = cvn.canvas_variation_id
LEFT JOIN tmp_canvas_step_names csn
	ON csn.canvas_step_id = b.canvas_step_id
LEFT JOIN tmp_canvas_names cn 
	ON cn.canvas_id = b.canvas_id
WHERE b.canvas_id IS NOT NULL
	AND b.sent_date::date >= DATEADD('day', -15, current_date);

END TRANSACTION;



GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;
GRANT select ON ALL TABLES IN SCHEMA dm_marketing TO redash_growth;
