DROP TABLE IF EXISTS tmp_risk_approval_funnel;
CREATE TEMP TABLE tmp_risk_approval_funnel
SORTKEY(submitted_date, created_date)
DISTKEY(submitted_date)
AS
WITH bank_account_snapshot AS 
(
	SELECT DISTINCT customer_id -- Note that this includes ALL customers who underwent verification (passed OR failed)
	FROM s3_spectrum_rds_dwh_order_approval.decision_tree_algorithm_results
	WHERE additional_reason = 'BAS service'
),
merkmal_customers AS
(
	SELECT DISTINCT user_id
	FROM ods_production.order_scoring os
	WHERE order_scoring_comments ilike '%Merkmal%'
),
merkmal AS 
(
	SELECT 
		os.order_id, 
		CASE 
			WHEN u.user_id is not null 
		 	 	THEN 'MerkmalTest' 
		 	ELSE os.order_scoring_comments 
		END AS new_order_scoring_comment,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_scored_at DESC) AS row_num 
	FROM ods_production.order_scoring os
	LEFT JOIN merkmal_customers u 
		ON os.user_id = u.user_id 
),
 cost_products_orders AS (
    SELECT
        o.order_id,
        s.category_name ,
        s.subcategory_name,
        s.product_sku ,
        s.product_name ,
        s.brand,
        SUM(quantity) OVER (partition by o.order_id, s.category_name) AS total_products_cat,
        SUM(total_price) OVER (partition by o.order_id, s.category_name) AS total_price_cat,
        sum(quantity) AS total_products,
        sum(total_price) AS total_price
    FROM master."order" o
        INNER JOIN ods_production.order_item s
           ON o.order_id = s.order_id
    WHERE completed_orders = 1 
		AND o.created_date > DATEADD(YEAR, -2, current_date)
    GROUP BY 1,2,3,4,5,6, quantity, total_price
)
   , main_order_category AS (
    SELECT
        order_id,
        category_name,
        subcategory_name,
        product_sku,
        product_name,
        brand,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY total_price_cat DESC, total_products_cat DESC, category_name) AS category_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id, category_name ORDER BY total_price DESC, total_products DESC, category_name) AS product_rank
    FROM cost_products_orders
),
mr_data_eu AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_eu_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
),
mr_data_us AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_us_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
)
SELECT DISTINCT
	o.order_id,
	o.burgel_risk_category,
	o.completed_orders,
	o.created_date::timestamp AS created_date,
	o.submitted_date::timestamp AS submitted_date,
	o.marketing_channel,
	o.marketing_campaign,
	oc.marketing_source,
	o.new_recurring,
	o.new_recurring_risk,
	o.order_journey,
	oj.order_journey_grouped,
	oj.order_journey_mapping_risk,
	o.schufa_class,
	o.device,
	cat.category_name,
	cat.subcategory_name,
	cat.brand,
	cat.product_sku,
	cat.product_name,
	o.store_label,
	o.store_commercial,
	o.store_type,
	sco.model_name,
	o.paid_date::timestamp AS paid_date,
	o.declined_reason,
	o.customer_id ,
	o.order_value ,
	CASE
     	WHEN c.customer_type='business_customer' THEN 'B2B'||' '|| o.store_country
     	WHEN o.store_short IN ('Partners Online','Partners Offline') THEN 'Partnerships'||' '|| CASE WHEN o.store_country='Germany' THEN o.store_country ELSE 'International' END
     	ELSE 'Grover'||' '|| CASE WHEN o.store_country='Germany' THEN o.store_country ELSE 'International' END
     END AS store_commercial_new,
     sq.new_order_scoring_comment,
     CASE
     	WHEN c.customer_type = 'normal_customer' THEN 'B2C customer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	WHEN c.customer_type = 'business_customer' THEN 'B2B Unknown Split'
     	ELSE 'Null'
     END AS freelancer_split,
     CASE 
     	WHEN bas.customer_id IS NOT NULL 
     		THEN 'BAS Verified' -- Includes all those who underwent BAS verification (passed or failed).
     	ELSE 'Not BAS Verified' 
     END AS customer_bas_verified,
     CASE
     	WHEN mreu.order_id IS NOT NULL OR mrus.order_id IS NOT NULL THEN 1
     	ELSE 0
     END AS order_went_to_mr
FROM master."order" o
LEFT JOIN master.customer c 
	ON o.customer_id=c.customer_id
LEFT JOIN (SELECT DISTINCT order_id, model_name 
	FROM ods_production.order_scoring) sco 
	ON o.order_id = sco.order_id
LEFT JOIN ods_production.order_journey oj 
	ON o.order_id = oj.order_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name 
LEFT JOIN bank_account_snapshot bas 
	ON o.customer_id = bas.customer_id  
LEFT JOIN merkmal sq
	ON o.order_id = sq.order_id
   AND sq.row_num = 1
LEFT JOIN main_order_category cat
	ON cat.order_id = o.order_id 
   AND cat.category_rank = 1
   AND cat.product_rank = 1
LEFT JOIN mr_data_eu mreu
	ON o.order_id = mreu.order_id
LEFT JOIN mr_data_us mrus
	ON o.order_id = mrus.order_id
LEFT JOIN ods_production.order_marketing_channel oc 
	ON o.order_id = oc.order_id
WHERE o.submitted_date > DATEADD(YEAR, -2, current_date)
	OR o.created_date > DATEADD(YEAR, -2, current_date)
;

BEGIN TRANSACTION;

DROP TABLE IF EXISTS dm_risk.approval_funnel;
CREATE TABLE dm_risk.approval_funnel AS
SELECT *
FROM tmp_risk_approval_funnel;

DROP TABLE tmp_risk_approval_funnel; 

END TRANSACTION;

GRANT SELECT ON dm_risk.approval_funnel TO tableau;
