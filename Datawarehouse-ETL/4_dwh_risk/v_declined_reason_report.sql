CREATE OR REPLACE VIEW dm_risk.v_declined_reasons_report AS
WITH mr_data_eu AS
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
	o.created_date,
	o.submitted_date,		
	o.customer_id,
	o.completed_orders,
	o.order_journey,
	o.store_label,
	o.store_commercial,
	o.store_type,
	o.new_recurring,
	o.new_recurring_risk,
	o.status,
	o.declined_orders,
	o.declined_reason,
	o.burgel_risk_category,
	o.schufa_class,
	o.initial_scoring_decision,
	o.is_in_salesforce,
	o.customer_type,
	o.voucher_type,
	o.retention_group,
	o.payment_method,
	o.marketing_channel,
	o.marketing_campaign,
	o.device,
	o.voucher_code,
	cat.category_name ,
	sq.new_order_scoring_comment,
	oj.order_journey_mapping_risk,
	case
     	when c.customer_type='business_customer' then 'B2B'||' '|| o.store_country
     	when o.store_short in ('Partners Online','Partners Offline') then 'Partnerships'||' '||case when o.store_country='Germany' then o.store_country else 'International' end
     	else 'Grover'||' '||case when o.store_country='Germany' then o.store_country else 'International' end
     end as store_commercial_new,
     c.company_type_name,
     CASE
     	WHEN c.customer_type = 'normal_customer' THEN 'B2C customer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
     	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	WHEN c.customer_type = 'business_customer' THEN 'B2B Unknown Split'
     	ELSE 'Null'
     END AS freelancer_split,
     CASE
     	WHEN mreu.order_id IS NOT NULL OR mrus.order_id IS NOT NULL THEN 1
     	ELSE 0
     END AS order_went_to_mr
FROM master."order" o
LEFT JOIN ods_production.order_journey oj 
	ON o.order_id = oj.order_id
LEFT JOIN master.customer c on o.customer_id=c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre ON c.company_type_name = fre.company_type_name 
LEFT JOIN (
	with a as (
	 select os.ordeR_id, 
	 case when u.user_id is not null then 'MerkmalTest' else os.order_scoring_comments end as new_order_scoring_comment,
	 row_number()over (partition by order_id order by order_scored_at desc) as row_num 
	 from ods_production.order_scoring os
	 left join(
	 select distinct user_id
	 from ods_production.order_scoring os where order_scoring_comments ilike '%Merkmal%') u 
	 on os.user_id=u.user_id )
	 select * from a where row_num =1) sq on o.order_id = sq.order_id
LEFT JOIN (
	SELECT 
		order_id, 
		category_name, 
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY total_price DESC, total_products DESC, category_name) rowno
	FROM (
		SELECT 
			order_id,
			category_name,
			COUNT(DISTINCT product_sku) AS total_products,
			sum(price) AS total_price
		FROM (
			SELECT 
				o.order_id,
				s.product_sku ,
				s.category_name ,
				s.price
			FROM master."order" o 
			INNER JOIN ods_production.order_item s 
			  ON o.order_id = s.order_id
			WHERE completed_orders = 1 AND o.created_date > DATEADD(YEAR, -2, current_date)
		) sub
		GROUP BY 1,2)
	) cat
	  ON cat.order_id = o.order_id 
	  AND cat.rowno = 1
LEFT JOIN mr_data_eu mreu
	ON o.order_id = mreu.order_id
LEFT JOIN mr_data_us mrus
	ON o.order_id = mrus.order_id
where o.created_date > DATEADD(YEAR, -2, current_date)
WITH NO SCHEMA BINDING;
