BEGIN TRANSACTION;

DELETE FROM stg_curated.nethone_data 
WHERE extracted_at > (
		SELECT MAX(extracted_at) 
		FROM stg_curated.nethone_data
	);

INSERT INTO stg_curated.nethone_data (
		created_at,
		updated_at,
		id, 
		customer_id ,
		order_id,
		profiling_reference,
		attempt_reference,
		score,
		advice,
		creation_timestamp,
		"data",
		extracted_at,
		"year",
		"month",
		"day",
		"hour"
 	)
SELECT 			
		created_at,
 		updated_at,
 		id, 
 		customer_id ,
 		order_id,
 		profiling_reference,
 		attempt_reference,
 		score,
 		advice,
 		creation_timestamp,
 		"data",
 		extracted_at,
  		"year",
 		"month",
 		"day",
 		"hour"
 FROM s3_spectrum_rds_dwh_order_approval.nethone_data
 WHERE 
 extracted_at > (
		SELECT MAX(extracted_at) 
		FROM stg_curated.nethone_data
	);

END TRANSACTION;
