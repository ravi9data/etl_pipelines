CREATE OR REPLACE VIEW dm_operations.v_ratiopoll_hats_after_return AS
WITH customer_list AS
(
	SELECT 
		customer_id,
		entity_id,
		MAX(created_at)::date AS created_date
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug IN ('after-return-rental-experience', 'after-return-rental-xp-reasons' , 'on-last-return-experience' , 'return-xp-reasons' , 'return-reasons' , 'after-return-likelihood-recommend')
		AND customer_id IS NOT NULL
		AND entity_id IS NOT NULL
	GROUP BY 1,2
)
, data_q1_rentexp AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'after-return-rental-experience'
)
, data_q2_afterreturnexp_reasons AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'after-return-rental-xp-reasons'
)
, data_q3_lastexp AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'on-last-return-experience'
)
, data_q4_returnexp_reasons AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'return-xp-reasons'
)
, data_q5_return_reasons AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'return-reasons'
)
, data_q6_recommend AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug, 
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'after-return-likelihood-recommend'
)
, data_joined AS 
(
SELECT 
	cl.customer_id,
	cl.entity_id AS return_shipment_id,
	cl.created_date,
	-- Q1 
	q1.vote_slug AS q1_rentexp,
	-- Q2
	q2.vote_slug AS q2_afterreturnexp_reasons,
	CASE 
		WHEN q2."comment" IS NOT NULL AND q2."comment" != ' ' 
			THEN q2."comment" 
	END AS q2_afterreturnexp_reasons_comment,
	-- Q3
	q3.vote_slug AS q3_lastexp,
	-- Q4
	q4.vote_slug AS q4_returnexp_reasons,
	CASE 
		WHEN q4."comment" IS NOT NULL AND q4."comment" != ' ' 
			THEN q4."comment" 
	END AS q4_returnexp_reasons_comment,
	-- Q5
	q5.vote_slug AS q5_return_reasons,
	CASE 
		WHEN q5."comment" IS NOT NULL AND q5."comment" != ' ' 
			THEN q5."comment" 
	END AS q5_return_reasons_comment,	
	-- Q6
	q6.vote_slug::text AS q6_recommend,
	-- DATA ENRICHING.
	s.category_name, 
	s.subcategory_name, 
	s.subscription_plan,
	o.store_country,
	c.customer_type,
     CASE
     	WHEN c.customer_type = 'normal_customer' THEN 'B2C customer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		WHEN c.customer_type = 'business_customer' THEN 'B2B Unknown Split'
     END AS customer_type_mapped,
    ROW_NUMBER() OVER (PARTITION BY cl.entity_id ORDER BY cl.created_date DESC) AS row_num  -- FINAL clean.
FROM customer_list cl
---- EACH QUESTION RESPONSE IS A LEFT JOIN
LEFT JOIN data_q1_rentexp q1
	ON q1.customer_id = cl.customer_id 
	AND q1.entity_id = cl.entity_id
	AND q1.row_num = 1
LEFT JOIN data_q2_afterreturnexp_reasons q2
	ON q2.customer_id = cl.customer_id
	AND q2.entity_id = cl.entity_id
	AND q2.row_num = 1
LEFT JOIN data_q3_lastexp q3
	ON q3.customer_id = cl.customer_id
	AND q3.entity_id = cl.entity_id
	AND q3.row_num = 1
LEFT JOIN data_q4_returnexp_reasons q4
	ON q4.customer_id = cl.customer_id
	AND q4.entity_id = cl.entity_id
	AND q4.row_num = 1	
LEFT JOIN data_q5_return_reasons q5
	ON q5.customer_id = cl.customer_id
	AND q5.entity_id = cl.entity_id
	AND q5.row_num = 1	
LEFT JOIN data_q6_recommend q6
	ON q6.customer_id = cl.customer_id
	AND q6.entity_id = cl.entity_id
	AND q6.row_num = 1	
---- ENRICHING DATA.
LEFT JOIN ods_operations.allocation_shipment al
	ON cl.entity_id = al.return_shipment_uid 
LEFT JOIN master.subscription s 
	ON al.subscription_id = s.subscription_id
LEFT JOIN master."order" o 
	ON s.order_id = o.order_id
LEFT JOIN master.customer c 
	ON o.customer_id = c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE cl.created_date >= DATEADD('month', -24, CURRENT_DATE)
)
SELECT *
FROM data_joined
WHERE row_num = 1  -- FINAL clean TO duplicate RETURN shipping id's (3 found , Nov 22).
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_operations.v_ratiopoll_hats_after_return TO redash_growth;
