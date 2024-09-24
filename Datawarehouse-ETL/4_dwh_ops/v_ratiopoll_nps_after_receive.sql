CREATE OR REPLACE VIEW dm_operations.v_ratiopoll_nps_after_receive AS
WITH customer_list AS
(
	SELECT 
		customer_id,
		entity_id,
		MAX(created_at::date) AS created_date
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug IN ('last-order-delivery-experience', 'where-found-about', 'which-social-media-channel', 'delivery-xp-reasons', 'likelihood-recommend')
		AND customer_id IS NOT NULL
		AND entity_id IS NOT NULL
	GROUP BY 1,2
)
, data_q1_lastexp AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'last-order-delivery-experience'
)
, data_q2_wherefound AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'where-found-about'
)
, data_q3_whichchannel AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'which-social-media-channel'
)
, data_q4_deliveryreasons AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug,
		"comment",
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'delivery-xp-reasons'
)
, data_q5_likelihoodrecommend AS
(
	SELECT 
		customer_id, 
		entity_id, 
		vote_slug, 
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM staging.spectrum_polls_votes pv
	WHERE poll_slug = 'likelihood-recommend'
)
, data_joined AS 
(
SELECT 
	cl.customer_id,
	cl.entity_id AS order_id,
	cl.created_date,
	-- Q1 (no comments available)
	q1.vote_slug AS q1_lastdeliveryexperience,
	-- Q2
	q2.vote_slug AS q2_wherefoundoutabout,
	CASE 
		WHEN q2."comment" IS NOT NULL AND q2."comment" NOT IN (' ') 
			THEN q2."comment" 
	END AS q2_wherefoundoutabout_comment,
	-- Q3
	q3.vote_slug AS q3_whichsmchannel,
	CASE 
		WHEN q3."comment" IS NOT NULL AND q3."comment" NOT IN (' ') 
			THEN q3."comment" 
	END AS q3_whichsmchannel_comment,
	-- Q4
	q4.vote_slug AS q4_deliveryxpreasons,
	CASE 
		WHEN q4."comment" IS NOT NULL AND q4."comment" NOT IN (' ') 
			THEN q4."comment" 
	END AS q4_deliveryxpreasons_comment,
	-- Q5 (no comments available)
	q5.vote_slug::text AS q5_likelihoodreccommend,
	-- DATA ENRICHING.
	o.store_country,
	c.customer_type,
    CASE
   		WHEN c.customer_type = 'normal_customer' THEN 'B2C'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B Freelancer'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B Non-Freelancer'
    END AS customer_type_mapped,
    ROW_NUMBER() OVER (PARTITION BY cl.entity_id ORDER BY cl.created_date DESC) AS row_num  -- Somehow there was still 2 duplicates AFTER previous cleaning. Implementing one FINAL clean.
FROM customer_list cl
---- ADDING EACH QUESTION RESPONSE AS A LEFT JOIN
LEFT JOIN data_q1_lastexp q1
	ON q1.customer_id = cl.customer_id 
	AND q1.entity_id = cl.entity_id
	AND q1.row_num = 1
LEFT JOIN data_q2_wherefound q2
	ON q2.customer_id = cl.customer_id
	AND q2.entity_id = cl.entity_id
	AND q2.row_num = 1
LEFT JOIN data_q3_whichchannel q3
	ON q3.customer_id = cl.customer_id
	AND q3.entity_id = cl.entity_id
	AND q3.row_num = 1
LEFT JOIN data_q4_deliveryreasons q4
	ON q4.customer_id = cl.customer_id
	AND q4.entity_id = cl.entity_id
	AND q4.row_num = 1	
LEFT JOIN data_q5_likelihoodrecommend q5
	ON q5.customer_id = cl.customer_id
	AND q5.entity_id = cl.entity_id
	AND q5.row_num = 1	
---- ENRICHING DATA.
LEFT JOIN master."order" o 
	ON cl.entity_id = o.order_id
LEFT JOIN master.customer c 
	ON cl.customer_id = c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE o.store_country IS NOT NULL -- ONLY 13 datapoints excluded here, June 22.
	AND cl.created_date >= DATEADD('month', -12, CURRENT_DATE)
)
SELECT *
FROM data_joined
WHERE row_num = 1
WITH NO SCHEMA BINDING; 
