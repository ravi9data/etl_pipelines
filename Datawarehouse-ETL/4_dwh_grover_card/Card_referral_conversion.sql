WITH loyalty_customer AS (
	SELECT 
		rec.host_id,
		code_created_date AS host_code_created_date,
		host_referred_date ,
		lc.number_of_referral,
		count(DISTINCT CASE WHEN submitted_date IS NOT NULL THEN guest_id END ) AS submitted_guest,
		count(DISTINCT CASE WHEN paid_date IS NOT NULL THEN guest_id END ) AS paid_guest,
		count(DISTINCT CASE WHEN loyalty_contract_start_date IS NOT NULL THEN guest_id END ) AS contract_started_guest
	FROM ods_referral.host_code_created rec 
	LEFT JOIN master_referral.host_guest_mapping hgm 
		ON hgm.host_id=rec.host_id
	LEFT JOIN master_referral.loyalty_customer lc 
		ON hgm.host_id=lc.customer_id
	--WHERE hgm.host_id='932342'
	GROUP BY 1,2,3,4
	)
,card_customer as (
	SELECT 
		customer_id
	WHERE first_card_created_date IS NOT NULL
	--AND customer_id='932342'
	)
SELECT
	host_id,
	host_code_created_date,
	host_referred_date ,
	COALESCE(number_of_referral,0) AS number_of_referral,
	submitted_guest,
	paid_guest,
	contract_started_guest,
	CASE WHEN cc.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS card_customer
FROM loyalty_customer lc 
LEFT JOIN card_customer cc 
ON lc.host_id = cc.customer_id 
WHERE card_customer IS TRUE  
WITH NO SCHEMA binding; 

