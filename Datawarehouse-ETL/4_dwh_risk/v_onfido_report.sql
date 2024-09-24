CREATE OR REPLACE VIEW dm_risk.v_onfido_report AS
WITH eu_data AS
(	
	SELECT 
		o.submitted_date::date AS submitted_date,
		o.customer_type,
		o.store_country ,
		onf.verification_state ,
		onf.merkmal ,
		COALESCE(onf.onfido_trigger, 'non trigger') AS onfido,
		count(DISTINCT o.customer_id) AS num_total_customers,
		COALESCE(SUM(onf.onfido_customer),0) AS num_onfido_customers
	FROM (SELECT 
			  customer_id, 
			  order_id, 
			  customer_type , 
			  store_country , 
			  submitted_date, 
			  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY submitted_date) AS rowno
	      FROM master."order" os 
	      WHERE store_country != 'United States' 
	      AND os.submitted_date  >= '2019-01-08'
	     ) o
	LEFT JOIN (SELECT * FROM
					(SELECT
						1 AS onfido_customer,
						oo.order_id,
						oo.customer_id AS cust_id,
						oo.submitted_date::date,
						os.verification_state,
						CASE WHEN os.order_scoring_comments ILIKE '%Merkmal%' THEN 'Merkmal' ELSE 'Non-Merkmal' END AS Merkmal,
						os.onfido_trigger,
						row_number() OVER (PARTITION BY customer_id ORDER BY submitted_date) AS row_num
					FROM master."order" oo
					LEFT JOIN ods_production.order_scoring os ON os.order_id = oo.order_id
					WHERE onfido_trigger IS NOT NULL) 
				WHERE row_num = 1) onf 
	  ON onf.cust_id = o.customer_id
	WHERE o.rowno = 1
	GROUP BY 1,2,3,4,5,6
),
us_data AS 
(
	WITH data_raw AS
	(
		SELECT
			*,
			-- id here is unique to the verification attempt (customer id may have several id's)
			ROW_NUMBER() OVER (PARTITION BY id, customer_id, verification_state ORDER BY consumed_at) AS row_num
		FROM stg_curated.risk_internal_us_risk_id_verification_request_v1
	),
	data_no_duplicates AS
	(
		SELECT *
		FROM data_raw
		WHERE row_num = 1
	)
	SELECT 
		o.submitted_date::date AS submitted_date,
		o.customer_type,
		'United States' AS store_country,
		CASE WHEN vc.customer_id IS NOT NULL THEN 'verified'
			WHEN vf.customer_id IS NOT NULL THEN 'failed'
			WHEN vip.customer_id IS NOT NULL THEN 'in_progress'
			WHEN vns.customer_id IS NOT NULL THEN 'not_started'
		ELSE NULL
		END AS verification_state,
		'Non-Merkmal' AS merkmal,
		CASE WHEN verification_state IS NOT NULL THEN 'trigger exists, but no data' ELSE 'non trigger' END AS onfido,
		COUNT(DISTINCT o.customer_id) AS num_total_customers,
		COUNT(DISTINCT CASE WHEN verification_state IS NOT NULL THEN o.customer_id END) AS num_onfido_customers
	FROM (SELECT 
			  customer_id, 
			  order_id, 
			  customer_type , 
			  store_country , 
			  submitted_date, 
			  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY submitted_date) AS row_num
	      FROM master."order" os 
	      WHERE store_country = 'United States'
	      AND os.submitted_date  >= '2021-09-01' -- note that we have 2016-18 orders (previous expansion attempt?)
	     ) o
	LEFT JOIN (SELECT DISTINCT customer_id FROM data_no_duplicates WHERE verification_state = 'verified') vc ON vc.customer_id = o.customer_id
	LEFT JOIN (SELECT DISTINCT customer_id FROM data_no_duplicates WHERE verification_state = 'failed') vf ON vf.customer_id = o.customer_id  
	LEFT JOIN (SELECT DISTINCT customer_id FROM data_no_duplicates WHERE verification_state = 'in_progress') vip ON vip.customer_id = o.customer_id
	LEFT JOIN (SELECT DISTINCT customer_id FROM data_no_duplicates WHERE verification_state = 'not_started') vns ON vns.customer_id = o.customer_id
	WHERE o.row_num = 1
	GROUP BY 1,2,3,4,5,6
)
SELECT * FROM eu_data
UNION
SELECT * FROM us_data
WITH NO SCHEMA BINDING;