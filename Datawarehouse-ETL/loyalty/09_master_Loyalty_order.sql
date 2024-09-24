drop table if exists master_referral.Loyalty_order;
create table master_referral.Loyalty_order AS
WITH src AS (
	SELECT 
			host_id::int AS customer_id,
			'referral_host' AS event_name,
			host_referred_date::timestamp AS event_timestamp,
			host_country AS country
	FROM master_referral.host_guest_mapping hgm 
	UNION 
	SELECT 
			guest_id::int AS customer_id,
			'referral_guest' AS event_name,
			guest_created_date::timestamp AS event_timestamp,
			guest_country  AS country
	FROM master_referral.host_guest_mapping hgm 
),src_idx AS (
	SELECT *,ROW_NUMBER()OVER (PARTITION BY customer_id ORDER BY event_timestamp) idx
	FROM src)-- WHERE customer_id='1940717'
,Loyalty_customer AS (
	SELECT 
	customer_id,
	event_timestamp as first_event_timestamp,
	event_name as first_event_name,
	 country AS country,
	count(hg.host_id) AS number_of_referral
		FROM src_idx s
		LEFT JOIN master_referral.host_guest_mapping hg
		ON hg.host_id=s.customer_id
	WHERE idx=1
	GROUP BY 1,2,3,4)	
,first_after_referral AS (
SELECT  
    c.customer_id AS customer_R
    ,'Referral' AS customer_classification
    ,first_event_timestamp AS first_referral_date
    ,first_event_name AS first_referral_event
    ,country
    ,min(CASE when first_event_name ='referral_guest' THEN 
    		CASE when first_event_timestamp>submitted_date THEN NULL 
    	ELSE submitted_date END 
    	ELSE submitted_date END ) AS first_submitted_date
    ,min(approved_date) AS first_approved_date
    ,min(paid_date) AS first_paid_date  
    ,min(CASE WHEN first_event_timestamp<submitted_date THEN submitted_date end) AS first_submitted_date_after_referral
    ,min(CASE WHEN first_event_timestamp<approved_date THEN approved_date  END) AS first_approved_date_after_referral
    ,min(CASE WHEN first_event_timestamp<paid_date THEN paid_date  END) AS first_paid_date_after_referral 
FROM ods_production."order" o 
INNER JOIN Loyalty_customer c 
	ON c.customer_id=o.customer_id 
	--AND c.first_event_timestamp<=o.created_date 
	--WHERE first_event_name ='referral_host'
GROUP BY 1,2,3,4,5
)
,first_not_referral AS (
	SELECT  
	    o.customer_id AS customer_NR
	    ,min(created_date) AS first_created_Date_NR
	    ,min(submitted_date) AS first_submitted_date_NR
	    ,min(approved_date) AS first_approved_date_NR
	    ,min(paid_date) AS first_paid_date_NR  
	FROM ods_production."order" o 
	LEFT JOIN Loyalty_customer c
		ON o.customer_id =c.customer_id
		WHERE c.customer_id IS NULL 
		--AND c.first_event_timestamp<=o.created_date 
		--WHERE c.customer_id='55688'
	GROUP BY 1
	)
,revoked_item AS (
	SELECT 
		guest_id,
		order_id,
		revoked_date AS revoked_date
	FROM ods_referral.guest_revoked 
	)
SELECT 
	o.order_id
	,o.customer_id
	,CASE WHEN far.customer_classification IS NULL 
		THEN 'Non Referral' 
	ELSE customer_classification 
	END AS customer_classification
	,first_referral_date
	,first_referral_event
	,o.created_Date
	,o.submitted_date
	,o.approved_date
	,o.paid_date
	,gr.revoked_date
	,canceled_date
	,COALESCE(country,o.store_country) AS country 
	,o.total_orders 
    ,o.order_value 
    ,o.voucher_code 
    ,o.voucher_type
    ,o.voucher_value 
    ,o.voucher_discount 
    ,r.new_recurring 
    ,m.marketing_channel 
    ,m.devicecategory AS device 
    ,st.store_name
    ,basket_size
    ,order_item_count
    ,1 AS cart_orders 
    ,ocl.cart_page_orders 
    ,ocl.completed_orders AS submitted_orders
    ,ocl.paid_orders 
    ,CASE WHEN o.created_date::date>=first_referral_date::Date THEN 1 ELSE 0 END is_order_created_after_referral
	,CASE WHEN submitted_date=first_submitted_date THEN 1 ELSE 0 END AS is_first_submitted_order_for_ref_user
	,CASE WHEN approved_date=first_approved_date THEN 1 ELSE 0 END AS is_first_approved_order_for_ref_user
	,CASE WHEN paid_date=first_paid_date THEN 1 ELSE 0 END AS is_first_paid_order_for_ref_user
	,CASE WHEN submitted_date=first_submitted_date_after_referral THEN 1 ELSE 0 END AS is_first_submitted_order_after_referral_for_ref_user
	,CASE WHEN approved_date=first_approved_date_after_referral THEN 1 ELSE 0 END AS is_first_approved_order_after_referral_for_ref_user
	,CASE WHEN paid_date=first_paid_date_after_referral THEN 1 ELSE 0 END AS is_first_paid_order_after_referral_for_ref_user
	,CASE WHEN submitted_date=first_submitted_date_NR THEN 1 ELSE 0 END AS is_first_submitted_order_for_non_ref_user
	,CASE WHEN approved_date=first_approved_date_NR THEN 1 ELSE 0 END AS is_first_approved_order_for_non_ref_user
	,CASE WHEN paid_date=first_paid_date_NR THEN 1 ELSE 0 END AS is_first_paid_order_for_non_ref_user
FROM ods_production."order" o 
LEFT JOIN first_after_referral far
	ON o.customer_id =far.customer_r
LEFT JOIN first_not_referral fnr
	ON o.customer_id =fnr.customer_nr
LEFT JOIN revoked_item gr 
	ON o.order_id =gr.ordeR_id
LEFT JOIN ods_production.order_retention_group r 
    ON r.order_id=o.order_id
LEFT JOIN ods_production.order_marketing_channel m 
    ON o.order_id = m.order_id
LEFT JOIN ods_production.store st
    ON st.id=o.store_id
LEFT JOIN ods_production.order_conversion_labels ocl 
    ON ocl.order_id=o.order_id;

GRANT SELECT ON master_referral.Loyalty_order TO tableau;
