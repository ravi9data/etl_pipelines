/* DROP TABLE dm_finance.dni_number_base  ;
CREATE TABLE dm_finance.dni_number_base  AS
SELECT
DISTINCT
user_id::int AS customer_id,
created_at,
identification_number,
identification_sub_type
FROM s3_spectrum_rds_dwh_api_production_sensitive.personal_identifications ; */ -- one time table


DELETE FROM dm_finance.dni_number_base
WHERE date(created_at)= current_date;

INSERT INTO dm_finance.dni_number_base
SELECT
user_id::int AS customer_id,
created_at,
identification_number,
identification_sub_type
FROM s3_spectrum_rds_dwh_api_production_sensitive.personal_identifications di
WHERE date(created_at)= current_date
AND user_id NOT IN (SELECT user_id FROM dm_finance.dni_number_base);


DROP TABLE dm_finance.personal_identifications ;
CREATE TABLE dm_finance.personal_identifications AS
SELECT
DISTINCT
db.customer_id,
db.created_at,
identification_number,
identification_sub_type,
ROW_NUMBER() OVER (PARTITION BY identification_number ORDER BY created_at DESC ) AS idx
FROM dm_finance.dni_number_base db
INNER JOIN (SELECT DISTINCT customer_id FROM master.subscription) AS s  /*to include the customers only with subscriptions */
ON s.customer_id=db.customer_id
;