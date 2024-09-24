
SET enable_case_sensitive_identifier TO true;

DROP TABLE IF EXISTS dm_risk.pbi_customer_tracking; 

CREATE TABLE dm_risk.pbi_customer_tracking AS 
WITH data_raw AS 
(
	SELECT
		"_airbyte_data"."Customer ID"::VARCHAR(25) AS customer_id,
		regexp_replace("_airbyte_data"."Amount Overdue** (PbI Adjusted)"::VARCHAR(55),'([^0-9.])','')::DECIMAL(12,2) AS amount_overdue,
		"_airbyte_data"."Account Owner"::VARCHAR(55) AS account_owner,
		REPLACE("_airbyte_data"."Comment category"::VARCHAR(55), '"', '') AS payment_expectation,
		"_airbyte_data"."Comment / Ticket Link"::VARCHAR(255) AS comments	
	FROM
		staging_airbyte_bi._airbyte_raw_b2b_payments_outstanding_01_12
)
, customer_expectation AS 
(
	SELECT
		customer_id,
		payment_expectation,
		SUM(amount_overdue) AS total_overdue,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_overdue DESC ) row_num
	FROM
		data_raw
	WHERE payment_expectation IS NOT NULL
	GROUP BY 1,2
)
, customer_comments AS 
(
	SELECT
		customer_id,
		comments,
		SUM(amount_overdue) AS total_overdue,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_overdue DESC ) row_num
	FROM
		data_raw
	WHERE comments IS NOT NULL 
	GROUP BY 1,2
)
, account_owner AS 
(
	SELECT
		customer_id,
		account_owner,
		SUM(amount_overdue) AS total_overdue,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_overdue DESC ) row_num
	FROM
		data_raw
	WHERE account_owner IS NOT NULL 
	GROUP BY 1,2
)
SELECT DISTINCT 
	dr.customer_id,
	ce.payment_expectation,
	cc.comments,
	ao.account_owner,
	SUM(dr.amount_overdue) AS total_overdue
FROM data_raw dr
LEFT JOIN customer_expectation ce
	ON ce.customer_id = dr.customer_id
	AND ce.row_num = 1
LEFT JOIN customer_comments cc
	ON cc.customer_id = dr.customer_id
	AND cc.row_num = 1
LEFT JOIN account_owner ao
	ON ao.customer_id = dr.customer_id
	AND ao.row_num = 1
GROUP BY 1,2,3,4
; 


SET enable_case_sensitive_identifier TO off;
