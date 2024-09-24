TRUNCATE TABLE marketing.voucherify_voucher_transactions;

INSERT INTO marketing.voucherify_voucher_transactions
WITH latest_vouchers AS (
    SELECT
        code,
        value,
        discount_type,
        campaign,
        category,
        COALESCE(start_date, creation_date) AS start_date,
        expiration_date,
        gift_balance,
        loyalty_card_balance,
        redemption_limit,
        redemption_count,
        active,
        qr_code,
        barcode,
        id,
        is_referral_code,
        creation_date,
        last_update_date,
        discount_amount_limit,
        campaign_id,
        additional_info,
        customer_id,
        discount_unit_type,
        discount_unit_effect,
        customer_source_id,
        validation_rules_id,
        allocation,
        asset,
        locale,
        "order",
        payment,
        recurring,
        request
    FROM
        marketing.voucherify_voucher
    WHERE 1 = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY code, id, campaign_id, validation_rules_id ORDER BY
    							CASE WHEN last_update_date IS NOT NULL THEN 0 ELSE 1 END,
                   				last_update_date DESC) = 1)


,check_voucher_code_pattern AS (
SELECT
	 NULLIF(split_part(code,'-',1),'') AS pattern1
	,NULLIF(split_part(code,'_',1),'') AS pattern2
	,NULLIF(split_part(code,'-',2),'') AS pattern3
	,LEFT(code,5) AS pattern4
	,count(code) OVER (PARTITION BY pattern1) AS number_of_codes_pat1
	,count(code) OVER (PARTITION BY pattern2) AS number_of_codes_pat2
	,count(code) OVER (PARTITION BY pattern3) AS number_of_codes_pat3
	,count(code) OVER (PARTITION BY pattern4) AS number_of_codes_pat4
	,code
FROM latest_vouchers
)

,validation_rules_cleanup AS (
SELECT
	*,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE (conditions, '{', ''), '$', ''), '"',''),'[',''), ']',''), '}', '') AS cleaned_conditions
FROM marketing.voucherify_validation_rules_extracted)


,pivoted_rules AS(
SELECT
	val_rule_id
	,LISTAGG(DISTINCT CASE WHEN property = 'store_code' THEN cleaned_conditions END , ', ') WITHIN GROUP (ORDER BY val_rule_id) AS store_codes
	,LISTAGG(DISTINCT CASE WHEN property = 'store_type' THEN cleaned_conditions END , ', ') WITHIN GROUP (ORDER BY val_rule_id) AS store_type
	,MAX(CASE WHEN property = 'ended_subscriptions' THEN cleaned_conditions END) AS ended_subscriptions
	,MAX(CASE WHEN property = 'processing_subscriptions' THEN cleaned_conditions END) AS processing_subscriptions
	,MAX(CASE WHEN property = 'amount_of_active_subscriptions' THEN cleaned_conditions END) AS amount_of_active_subscriptions
	,MAX(CASE WHEN property = 'lowest_subscription_plan' THEN cleaned_conditions END) AS lowest_subscription_plan
	,MAX(CASE WHEN property = 'items_total' THEN cleaned_conditions END) AS items_total
FROM validation_rules_cleanup
GROUP BY val_rule_id)

,rules_enrichment AS(
SELECT
	val_rule_id
	,CASE
		WHEN ended_subscriptions = 'is: 0'
		AND processing_subscriptions = 'is: 0'
		AND amount_of_active_subscriptions = 'is: 0' THEN 'New Customers'
		WHEN amount_of_active_subscriptions = 'more_than: 0'
		AND ended_subscriptions = 'more_than: 0' THEN 'Recurring Customers'
		ELSE 'All Customers'
	END AS targeted_users
	,CASE WHEN store_type LIKE '%is: retail%' THEN 'Retail'
		WHEN store_codes LIKE '%is: business%' THEN 'B2B'
		WHEN store_codes LIKE '%is: de%'
		OR store_codes LIKE '%is: es%'
		OR store_codes LIKE '%is: us%'
		OR store_codes LIKE '%is: at%'
		OR store_codes LIKE '%is: nl%' THEN 'B2C'
	END AS customer_type
	,CASE WHEN store_codes = 'is: de' THEN 'DE'
		 WHEN store_codes = 'is: es' THEN 'ES'
		 WHEN store_codes = 'is: at' THEN 'AT'
		 WHEN store_codes = 'is: nl' THEN 'NL'
		 WHEN store_codes = 'is: us' THEN 'US'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: es%' AND store_codes LIKE '%is: at%' AND store_codes LIKE '%is: nl%' THEN 'DE & ES & AT & NL'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: es%' AND store_codes LIKE '%is: at%' THEN 'DE & ES & AT'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: es%' AND store_codes LIKE '%is: nl%' THEN 'DE & ES & NL'
		 WHEN store_codes LIKE '%is: es%' AND store_codes LIKE '%is: at%' AND store_codes LIKE '%is: nl%' THEN 'ES & AT & NL'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: at%' AND store_codes LIKE '%is: nl%' THEN 'DE & AT & NL'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: es%' THEN 'DE & ES'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: at%' THEN 'DE & AT'
		 WHEN store_codes LIKE '%is: de%' AND store_codes LIKE '%is: nl%' THEN 'DE & NL'
		 WHEN store_codes LIKE '%is: es%' AND store_codes LIKE '%is: at%' THEN 'ES & AT'
		 WHEN store_codes LIKE '%is: es%' AND store_codes LIKE '%is: nl%' THEN 'ES & NL'
		 WHEN store_codes LIKE '%is: at%' AND store_codes LIKE '%is: nl%' THEN 'AT & NL'
		 END AS country
	,CASE
	 	WHEN lowest_subscription_plan ='more_than: 5' THEN 6
	 	WHEN lowest_subscription_plan ='more_than: 2' THEN 3
	 	WHEN lowest_subscription_plan ='is: 12' THEN 12
	 	WHEN lowest_subscription_plan ='is: 6' THEN 6
	 	WHEN lowest_subscription_plan ='is: 3' THEN 3
	 	WHEN lowest_subscription_plan ='is: 1' THEN 1
	 	WHEN lowest_subscription_plan ='more_than: 10' THEN 11
	 	WHEN lowest_subscription_plan ='more_than: 11' THEN 12
	 	WHEN lowest_subscription_plan ='more_than: 9' THEN 10 END AS lowest_subscription_plan
	 ,items_total
FROM pivoted_rules
)

,redemptions AS (
	SELECT
	 id
	,voucher_code AS v_code
	,"result"
	,"object"
	,store_code
	,CASE WHEN POSITION('_' IN store_code) > 0 THEN split_part(store_code, '_',1) ELSE store_code END AS store_code_new
	,CASE
		WHEN store_type = 'retail' THEN 'Retail'
		WHEN store_code_new = 'business' THEN 'B2B'
		WHEN store_code_new = 'de'
		OR store_code_new = 'es'
		OR store_code_new = 'nl'
		OR store_code_new = 'at'
		OR store_code_new = 'us' THEN 'B2C'
		END AS customer_type
	,CASE
		WHEN ended_subscriptions = 0
			AND processing_subscriptions = 0
			AND amount_of_active_subscriptions = 0 THEN 'New Customers'
			WHEN amount_of_active_subscriptions > 0
			AND ended_subscriptions > 0 THEN 'Recurring Customers'
			ELSE 'All Customers'
		END AS targeted_users
FROM marketing.voucherify_redemption
)

SELECT
	CASE WHEN POSITION('-' IN v.code) > 0 OR POSITION('_' IN v.code) > 0 OR p.number_of_codes_pat4 >10 THEN 'Bulk' ELSE 'Standalone' END AS voucher_type
	,v.code AS voucher_code
	,CASE WHEN v.recurring THEN 'recurring' ELSE 'first_month_only' END AS voucher_length
	,v.start_date AS start_date
	,v.expiration_date AS end_date
	,datediff('day', start_date::timestamp, end_date::timestamp)
	,category AS voucher_details
	,discount_unit_effect AS discount_application
	,discount_type AS discount_type
	,v.value AS discount_value
	,COALESCE(vr.targeted_users, r.targeted_users) AS targeted_users
	,COALESCE(vr.customer_type, r.customer_type) AS customer_type
	,COALESCE(vr.country, r.store_code) AS store_code
	,vr.items_total AS item_number_limitation
	,vr.lowest_subscription_plan AS subscription_plan_limitation
	,NULL AS product_limitation
	,r.id AS redemption_id
	,r.RESULT AS redemption_result
	,r."object" AS redemption_object
FROM latest_vouchers v
INNER JOIN check_voucher_code_pattern p ON v.code = p.code
LEFT JOIN rules_enrichment vr ON v.validation_rules_id = vr.val_rule_id
LEFT JOIN redemptions r ON v.code = r.v_code;