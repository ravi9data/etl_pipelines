CREATE OR REPLACE VIEW dm_operations.v_ratiopoll_return_flow AS
WITH data_raw AS
(
SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY entity_id, poll_slug ORDER BY consumed_at DESC, created_at) AS row_num
FROM staging.spectrum_polls_votes spv 
WHERE poll_slug ILIKE '%return-flow%' 
	AND entity_type = 'subscription' -- FOR SOME reason there ARE 3 ORDER id entries (170k subs entries). Removing these.
),
data_cleaned AS
(
SELECT *
FROM data_raw 
WHERE row_num = 1
),
data_combined AS
(
SELECT
	dc.customer_id,
	dc.entity_id AS subscription_id,
	dc.created_at::date AS date_voted,
	dc.vote_slug AS Q1_return_reason,
	dw.vote_slug AS Q2_dont_want_reason,
	dl.vote_slug AS Q3_dont_like_reason,
	-- Data Enriching
	COALESCE(s.order_id, s2.order_id) AS order_id, -- We need complete ORDER id TO EXTRACT SOURCE country.
	c.customer_type,
	CASE
   		WHEN c.customer_type = 'normal_customer' THEN 'B2C'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B Freelancer'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B Non-Freelancer'
    END AS customer_type_mapped
FROM data_cleaned dc
LEFT JOIN (SELECT * FROM data_cleaned WHERE poll_slug = 'return-flow.dont-want-to-rent-reason') dw ON dw.entity_id =  dc.entity_id AND dw.customer_id = dc.customer_id 
LEFT JOIN (SELECT * FROM data_cleaned WHERE poll_slug = 'return-flow.dont-like-device-reason') dl ON dl.entity_id =  dc.entity_id AND dl.customer_id = dc.customer_id
LEFT JOIN master.subscription s 
	ON dc.entity_id = s.subscription_id
LEFT JOIN ods_production.subscription s2
	ON dc.entity_id = s2.subscription_bo_id 
LEFT JOIN master.customer c
	ON dc.customer_id = c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE dc.poll_slug = 'return-flow.reason'
	AND dc.created_at >= DATE_TRUNC('month', DATEADD('month', -12, CURRENT_DATE))
),
discarded_votes AS
(
SELECT COUNT(*)
FROM data_cleaned
WHERE created_at < DATE_TRUNC('month', DATEADD('month', -12, CURRENT_DATE))
)
SELECT
	dc.*,
	ROW_NUMBER() OVER (ORDER BY date_voted ASC) + (SELECT * FROM discarded_votes) AS vote_count,
	o.store_country -- We have TO enrich IN this SECOND step IN ORDER TO GET complete ORDER ids IN previous step.
FROM data_combined dc
LEFT JOIN master."order" o
	ON o.order_id = dc.order_id
ORDER BY vote_count ASC
WITH NO SCHEMA BINDING; 