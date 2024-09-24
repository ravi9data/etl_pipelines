BEGIN;

DELETE FROM machine_learning.userid_session_id_matching
WHERE
      creation_date >= ${START_FROM}::DATE
      AND creation_date <= ${END_TO}::DATE;


INSERT INTO machine_learning.userid_session_id_matching (
  	row_id,
    user_id,
    user_id2,
    creation_time,
    creation_date
)
SELECT
	row_id,
    user_id,
    user_id2,
    creation_time,
    creation_date
FROM trans_dev.userid_session_id_matching_transformed;

COMMIT;
