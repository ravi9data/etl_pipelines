--NOTE: DO NOT CHANGE EXISTING COLUMN NAMES
BEGIN;

DROP TABLE IF EXISTS tmp_passive_conditions;
CREATE TEMP TABLE tmp_passive_conditions AS
SELECT	
	s.customer_id,	
	s.subscription_id,	
	s.status,	
	CASE	
		WHEN cancellation_reason_new IN ('RETURNED EARLY - Failed Payments') THEN TRUE	
		ELSE FALSE	
		END AS cancellation_other,	
	CASE	
		WHEN cancellation_reason_new IN ('RETURNED EARLY', 'REVOCATION', 'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST') THEN TRUE	
		WHEN scr.cancellation_reason IN ('cancelled by customer', 'customer request', 'customer request via cs')	
		AND cancellation_reason_new = 'CANCELLED BEFORE SHIPMENT' THEN TRUE	
		ELSE FALSE	
		END AS customer_cancellation,	
	CASE	
		WHEN cancellation_reason_new = 'FAILED DELIVERY' THEN TRUE	
		ELSE FALSE	
		END AS failed_delivery,	
	CASE	
		WHEN cancellation_reason_new = 'DEBT COLLECTION' AND scr.cancellation_date < current_date - 30 THEN TRUE	
		ELSE FALSE	
		END AS debt_collection,	
	CASE	
		WHEN decline_reason_new = 'Model Decline' THEN TRUE	
		WHEN decline_reason_new = 'Suspicious Activity' THEN TRUE	
		WHEN decline_reason_new = 'Insufficient Credit Scores' THEN TRUE	
		WHEN decline_reason_new IN ('Blacklist','Blacklisted') THEN TRUE	
		ELSE FALSE	
		END AS declined_users
    FROM ods_production.subscription s
        LEFT JOIN ods_production.subscription_cancellation_reason scr ON scr.subscription_id = s.subscription_id
        LEFT JOIN ods_production.order_decline_reason dr ON dr.order_id = s.order_id;

       
DROP TABLE IF EXISTS tmp_active_customers;
CREATE TEMP TABLE tmp_active_customers AS
SELECT DISTINCT	
	s.customer_id	
FROM master.subscription s	
LEFT JOIN ods_production.payment_subscription sp	
	ON s.subscription_id = sp.subscription_id	
WHERE s.status = 'ACTIVE'	
	AND sp.payment_type = 'FIRST'	
	AND sp.status = 'PAID';


DROP TABLE IF EXISTS tmp_rfm_customer_segment;
CREATE TEMP TABLE tmp_rfm_customer_segment AS
    SELECT customer_id, 
        rfm_segmentation AS rfm_segment,
        (recency * 100 + frequency * 10 + monetary) as rfm_score,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY "date" DESC) AS rn
    FROM dm_marketing.customer_rfm_segmentation_historical;

  
DROP TABLE IF EXISTS tmp_passive_customers;
CREATE TEMP TABLE tmp_passive_customers AS
	SELECT a.customer_id	
	FROM tmp_passive_conditions a	
	LEFT JOIN tmp_active_customers b USING (customer_id)	
	WHERE b.customer_id IS NULL	
	GROUP BY 1	
	HAVING COUNT(CASE WHEN customer_cancellation = TRUE THEN subscription_id END) >= 3	
	--	
	UNION	
	--	
	SELECT a.customer_id	
	FROM tmp_passive_conditions a	
	LEFT JOIN tmp_active_customers b USING (customer_id)	
	WHERE b.customer_id IS NULL	
	GROUP BY 1	
	HAVING COUNT(CASE WHEN failed_delivery = TRUE THEN subscription_id END) >= 3	
	--	
	UNION	
	--	
	SELECT DISTINCT a.customer_id	
	FROM tmp_passive_conditions a	
	LEFT JOIN tmp_active_customers b USING (customer_id)	
	WHERE b.customer_id IS NULL	
	AND debt_collection = TRUE	
	--	
	UNION	
	--	
	SELECT DISTINCT a.customer_id	
	FROM tmp_passive_conditions a	
	LEFT JOIN tmp_active_customers b USING (customer_id)	
	WHERE b.customer_id IS NULL	
	AND cancellation_other = TRUE	
	--	
	UNION	
	--	
	SELECT DISTINCT ct.customer_id::int	
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1 ct	
	LEFT JOIN tmp_active_customers b ON ct.customer_id::int = b.customer_id::int	
	WHERE tag_id = '1'	
	AND b.customer_id IS NULL;


DROP TABLE IF EXISTS tmp_max_cancellation_date;
CREATE TEMP TABLE tmp_max_cancellation_date AS
	SELECT	
		s.customer_id,	
		max(s.cancellation_date) AS max_cancellation_date,	
		max(CASE WHEN s.cancellation_date < dateadd('month', -1, current_date) THEN s.cancellation_date END) AS max_cancellation_date_previous_month, -- 30 days ago	
		max(CASE WHEN s.status = 'ACTIVE' THEN s.minimum_cancellation_date END)::date AS max_minimum_cancellation_date
	FROM ods_production.subscription s	
	LEFT JOIN ods_production.payment_subscription sp	
		ON s.subscription_id = sp.subscription_id	
	LEFT JOIN ods_production.subscription_cancellation_reason c	
		ON s.subscription_id = c.subscription_id	
	WHERE sp.payment_type = 'FIRST'	
		AND sp.status = 'PAID'	
		AND coalesce(c.cancellation_reason_churn,'active') NOT IN ('failed delivery')	
	GROUP BY 1;


DROP TABLE IF EXISTS tmp_active_subscriptions_previous_month;
CREATE TEMP TABLE tmp_active_subscriptions_previous_month AS
  SELECT
      sh.customer_id,
      COUNT(DISTINCT sh.subscription_id) AS active_subscriptions_previous_month
  FROM master.subscription_historical sh
  WHERE sh.status = 'ACTIVE'
    AND sh.date = DATEADD('month', -1, current_date)::date
  GROUP BY 1
;


DROP TABLE IF EXISTS customer_final;
CREATE TEMP TABLE customer_final AS

SELECT
  DISTINCT t1.customer_id,
  t1.company_id,
  t1.created_at,
  greatest(
    t1.updated_at,
    ah.updated_at,
    s.updated_at,
    od.updated_at
    --t2.updated_at
  ) as updated_at,
  --t1.gender,
  t1.subscription_limit,
  t1.subscription_limit_change_date,
  t1.customer_type,
  t1.company_name,
  t1.company_status,
  t1.company_type_name,
  t1.company_created_at,
  t1.billing_country,
  t1.shipping_country,
  t1.billing_city,
  t1.billing_zip,
  t1.shipping_city,
  t1.shipping_zip,
  t1.signup_language,
  t1.default_locale,
  t1.bundesland,
  t1.referral_code,
  t1.email_subscribe  AS email_subscribe,
  initial_subscription_limit,
  subscription_limit_defined_date,
  customer_scoring_result,
  burgel_score,
  burgel_score_details,
  burgel_person_known,
  burgel_address_details,
  verita_score,
  verita_person_known_at_address,
  fraud_type,
  case when  t2.trust_type = 'blacklisted'
        or   t2.tag_name = 'blacklist'
        or   t2.is_blacklisted is true
        then 'blacklisted'
        when t2.trust_type = 'whitelisted'
        or   t2.tag_name = 'whitelist'
        or   t2.is_whitelisted is true
        then 'whitelisted'
        else t2.trust_type
  end as trust_type,
  min_fraud_detected,
  max_fraud_detected,
  t2.schufa_class,
  od.carts as orders,
  od.submitted_orders as completed_orders,
  od.paid_orders,
  od.declined_orders,
  od.max_cart_date as last_order_created_date,
  od.last_cart_product_names,
  od.max_submitted_order_date,
  ah.delivered_allocations,
  ah.returned_allocations,
  ah.outstanding_purchase_price,
  s.active_subscription_product_names,
  s.active_subscription_subcategory,
  s.active_subscription_category,
  s.active_subscription_brand,
  s.ever_rented_asset_purchase_price,
  s.active_subscription_Value,
  s.committed_subscription_value,
  coalesce(s.active_subscriptions, 0) as active_subscriptions,
  s.subscription_revenue_due,
  s.subscription_revenue_paid,
  s.subscription_revenue_refunded,
  s.subscription_revenue_chargeback,
  s.payment_count,
  s.paid_subscriptions,
  s.refunded_subscriptions,
  s.failed_subscriptions,
  s.chargeback_subscriptions,
  t2.burgel_risk_category::VARCHAR(15),
  coalesce(s.subscriptions, 0) as subscriptions,
  s.start_date_of_first_subscription,
  s.first_subscription_store,
  s.first_subscription_acquisition_channel,
  s.second_subscription_store,
  s.subscription_durations,
  s.first_subscription_duration,
  s.second_subscription_duration,
  s.subs_wearables,
  s.subs_drones,
  s.subs_cameras,
  s.subs_phones_and_tablets,
  s.subs_computers,
  s.subs_gaming,
  s.subs_audio,
  s.subs_other,
  s.first_subscription_product_category,
  s.second_subscription_product_category,
  S.ever_rented_products,
  s.ever_rented_sku,
  s.ever_rented_brands,
  s.ever_rented_subcategories,
  s.ever_rented_categories,
  s.total_cashflow as clv,
  od.voucher_usage,
  s.minimum_cancellation_Date,
  s.minimum_cancellation_product,
  s.subs_pag,
  s.subs_1m,
  s.subs_3m,
  s.subs_6m,
  s.subs_12m,
  s.subs_24m,
  s.is_bad_customer,
  ac.customer_acquisition_cohort,
  ac.subscription_id as customer_acquisition_subscription_id,
  ac.customer_acquisition_rental_plan,
  ac.customer_acquisition_category_name,
  ac.customer_acquisition_subcategory_name,
  ac.customer_acquisition_product_brand,
  rfm.rfm_score,
  case
  when rfm.rfm_segment is null then 'No_Orders'
  else rfm.rfm_segment
  end as rfm_segment,
  coalesce(od.signup_country, 'never_add_to_cart') as signup_country,
  s.max_cancellation_date,
  ah.max_asset_delivered as max_asset_delivered_at,
  case 
   when coalesce(od.submitted_orders,0)<=0 then 'Registered'
   when coalesce(s.active_subscriptions,0)>=1 then 'Active'
   when coalesce(s.active_subscriptions,0)=0 and s.max_cancellation_date>=dateadd('month', -6, current_date) then 'Inactive'
   when coalesce(s.active_subscriptions,0)=0 and s.max_cancellation_date<dateadd('month', -6, current_date) then 'Lapsed'
  else 'Passive'
  end as crm_label,
  t1.profile_status,
  case
    when t1.age < 18 then 'under 18'
    when t1.age between 18 and 22 then '[18-22]'
    when t1.age between 23 and 27 then '[23-27]'
    when t1.age between 28 and 32 then '[28-32]'
    when t1.age between 33 and 37 then '[33-37]'
    when t1.age between 38 and 42 then '[38-42]'
    when t1.age between 43 and 47 then '[43-47]'
    when t1.age between 48 and 52 then '[48-52]'
    when t1.age between 53 and 57 then '[53-57]'
    when t1.age between 58 and 62 then '[58-62]'
    when t1.age >= 63 then '> 62'
    else 'n/a'
  end as age,
  CASE 
	WHEN ac.customer_acquisition_cohort >= dateadd('month', -1, current_date) THEN 'Active - New Users'	
	WHEN coalesce(s.active_subscriptions,0) >= 1 AND mmcd.customer_id IS NOT NULL THEN 'Active - Potential Churners'	
	WHEN coalesce(s.active_subscriptions,0 )>= 1	
		AND coalesce(aspm.active_subscriptions_previous_month,0) = 0
		--AND mcd.max_cancellation_date_previous_month >= dateadd('day',-30,current_date)::date-30*6	
			THEN 'Active - Reactivated'	
	WHEN coalesce(s.active_subscriptions,0) >= 1 THEN 'Active'	
	WHEN coalesce(s.active_subscriptions,0) = 0 AND mcd.max_cancellation_date >= dateadd('month', -1, current_date)
			THEN 'Inactive - Recently Churners'	
	WHEN coalesce(s.active_subscriptions,0) = 0 AND mcd.max_cancellation_date < dateadd('month', -1, current_date) AND mcd.max_cancellation_date >= dateadd('month', -6, current_date)
			THEN 'Inactive - Churners 1-6 months'	
	WHEN coalesce(s.active_subscriptions,0) = 0 AND mcd.max_cancellation_date < dateadd('month', -6, current_date)	
			THEN 'Inactive'	
	WHEN coalesce(s.active_subscriptions,0) = 0 AND pc.customer_id IS NOT NULL THEN 'Unhealthy'	
	WHEN coalesce(s.active_subscriptions,0) = 0 AND mcd.customer_id IS NULL THEN 'Registered'	
	END AS  crm_label_braze,
  s.ever_rented_variant_sku
FROM ods_production.customer t1 --check
LEFT JOIN ods_production.customer_allocation_history ah --check
  ON ah.customer_id = t1.customer_id
LEFT JOIN ods_production.customer_subscription_details s --check
  ON s.customer_id = t1.customer_id
LEFT JOIN ods_production.customer_orders_details od --check
  ON od.customer_id = t1.customer_id
LEFT JOIN ods_production.CUSTOMER_ACQUISITION_COHORT AC 
	on AC.customer_id = t1.customer_id
LEFT JOIN tmp_rfm_customer_segment rfm 
	ON rfm.customer_id = t1.customer_id 
	AND rfm.rn = 1
LEFT JOIN ods_production.customer_scoring t2
  ON t1.customer_id = t2.customer_id
LEFT JOIN tmp_passive_customers pc 
	ON t1.customer_id = pc.customer_id
LEFT JOIN tmp_active_subscriptions_previous_month aspm	
	ON aspm.customer_id = t1.customer_id
LEFT JOIN tmp_max_cancellation_date mcd	
	ON mcd.customer_id = t1.customer_id	
LEFT JOIN tmp_max_cancellation_date mmcd	
	ON t1.customer_id = mmcd.customer_id	
	AND mmcd.max_minimum_cancellation_date::date <= dateadd('day',30, current_date) --- NEW	
	AND mmcd.max_minimum_cancellation_date IS NOT NULL
;

DELETE FROM master.customer where 1=1;

INSERT into master.customer
select * from customer_final;
COMMIT;

