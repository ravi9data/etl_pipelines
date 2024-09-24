CREATE OR REPLACE VIEW dm_product.v_ratiopoll_report_checkout_satisfaction AS
WITH order_submission_secondstage AS
(
SELECT 
	*, 
	'd0abc083-324c-4cba-950a-45a60aa92c98' AS poll_id_firststage,
	ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY f.created_at, f.consumed_at DESC) AS row_num_sec -- Cleaning duplicates WITH row_num.
FROM staging.spectrum_polls_votes f
WHERE poll_version_id = '2a04e112-be0a-4351-86a1-779884d7a5bc'
),
full_data AS
(
SELECT DISTINCT
	pv.customer_id,
	pv.created_at::date AS date_voted,
	pv.entity_id,
	pv.entity_type,
	pv.vote_slug AS sentiment_vote, -- sentiment vote INITIALLY stage
	f.vote_slug AS reason_vote, -- feedback vote INITIALLY , introduce CASE WHEN WHEN introducing later polls (+ 3rd stage votes, etc. if needed)
	CASE
		WHEN pv."comment" <> ' '  THEN pv."comment"
		WHEN pv."comment" = ' ' THEN f."comment"
		WHEN pv."comment" IS NULL THEN f."comment"
	END AS user_comment,
	c.customer_type,
	CASE
   		WHEN c.customer_type = 'normal_customer' THEN 'B2C'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B Freelancer'
   		WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B Non-Freelancer'
    END AS customer_type_mapped,
	c.signup_country,
	o.created_date,
	o.total_orders,
	o.order_value,
	o.store_type,
	o.new_recurring,
	o.device,
	o.order_item_count,
	o.basket_size,
	o.cancellation_reason,
	o.declined_reason,
	o.store_short,
	o.store_commercial,
	o.store_country,
	pv.poll_version_id AS pollid_firststage,
	ROW_NUMBER() OVER (PARTITION BY pv.entity_id ORDER BY pv.created_at, pv.consumed_at DESC) AS row_num
FROM staging.spectrum_polls_votes pv
-- JOINS: ADDING 2nd, 3rd, etc. Stage Poll Votes
LEFT JOIN (SELECT * FROM order_submission_secondstage WHERE row_num_sec = 1) f -- Cleaning duplicates WITH row_num.
	ON pv.customer_id  = f.customer_id 
	AND pv.entity_id = f.entity_id 
	AND pv.poll_version_id = f.poll_id_firststage 
-- JOINS: DATA ENRICHING
LEFT JOIN master."order" o ON pv.entity_id = o.order_id
LEFT JOIN master.customer c ON pv.customer_id::VARCHAR(55) = c.customer_id::VARCHAR(55)
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE pv.poll_version_id IN -- Only Add FIRST STAGE poll id for each Poll Campaign (e.g. Sentiment poll)
	('d0abc083-324c-4cba-950a-45a60aa92c98') -- Sentiment vote DATA
	AND pv.created_at >= DATE_TRUNC('month', DATEADD('month', -12, CURRENT_DATE))
)
SELECT *
FROM full_data
WHERE row_num = 1 -- Cleaning duplicates WITH row_num. This reduced ROW count FROM 180434 rows before duplicate removal TO 130180 rows AFTER (on 5/5/22).
WITH NO SCHEMA BINDING;