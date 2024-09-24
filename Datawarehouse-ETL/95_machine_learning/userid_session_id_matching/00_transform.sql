DROP TABLE IF EXISTS trans_dev.userid_session_id_matching_transformed;

CREATE TABLE trans_dev.userid_session_id_matching_transformed DISTKEY (row_id) AS (
WITH ordered AS (
	SELECT
		md5(user_id || user_id2 || creation_time) as row_id,
		user_id,
		user_id2,
		creation_time,
		creation_time::DATE as creation_date,
		ROW_NUMBER () OVER (PARTITION BY row_id ORDER BY creation_time DESC) AS row_num
	FROM trans_dev.userid_session_id_matching_rds
	)
SELECT
    row_id,
    user_id,
    user_id2,
    creation_time,
    creation_time::DATE As creation_date
FROM ordered
WHERE
	row_num=1
);
