TRUNCATE TABLE dm_marketing.voucherify_voucher_enriched;

INSERT INTO  dm_marketing.voucherify_voucher_enriched
WITH latest_vouchers AS (
    SELECT
        code,
        value,
        discount_type,
        campaign,
        category,
        COALESCE(start_date, creation_date) AS voucher_start_date,
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
                   				last_update_date DESC) = 1 )

,check_voucher_code_pattern AS (
SELECT
	 NULLIF(split_part(code,'-',1),'') AS pattern1 --pattern1 = 00F6BkYGN26 for 00F6BkYGN26-ESP, pattern1 = N26 for N26-3ulUE8wC
	,NULLIF(split_part(code,'_',1),'') AS pattern2
	,NULLIF(split_part(code,'-',2),'') AS pattern3 --pattern3 = ESP for 00F6BkYGN26-ESP, pattern3 = 3ulUE8wC for N26-3ulUE8wC
	,LEFT(code,5) AS pattern4
	--we need to check which pattern is most common, so we can use it for grouping the bulk codes
	--so for 00F6BkYGN26-ESP we use pattern3 = ESP, for N26-3ulUE8wC we use pattern1 = N26
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
FROM marketing.voucherify_validation_rules_extracted )


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

,validation_rules AS(
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
	,CASE
		WHEN store_type LIKE '%is: retail%' THEN 'Retail'
		WHEN store_codes LIKE '%is: business%' THEN 'B2B'
		WHEN store_codes LIKE '%is: de%'
		OR store_codes LIKE '%is: es%'
		OR store_codes LIKE '%is: us%'
		OR store_codes LIKE '%is: at%'
		OR store_codes LIKE '%is: nl%' THEN 'B2C'
	END AS customer_type
	,CASE
		WHEN store_codes = 'is: de' THEN 'DE'
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
	,CASE
	    WHEN voucher_type = 'Bulk' THEN
	        CASE
	            WHEN POSITION('-' IN v.code) > 0 THEN
	                CASE
	                    WHEN number_of_codes_pat1 > number_of_codes_pat3 THEN pattern1
	                    WHEN number_of_codes_pat1 < number_of_codes_pat3 THEN pattern3
	                END
	            WHEN POSITION('_' IN v.code) > 0 THEN
	                pattern2
	            ELSE
	                pattern4
	        END
	    WHEN voucher_type = 'Standalone' THEN
	        v.code
	END AS voucher_code
	,max(CASE WHEN v.recurring THEN 'recurring' ELSE 'first_month_only' END) AS voucher_length
	,min(v.voucher_start_date) AS start_date
	,max(v.expiration_date) AS end_date
	,datediff('day',start_date::timestamp, end_date::timestamp) AS days_active
	,LISTAGG(DISTINCT category, ', ') WITHIN GROUP (ORDER BY voucher_code) AS voucher_details
	,max(discount_unit_effect) AS discount_application
	,max(discount_type) AS discount_type
	,max(v.value) AS discount_value
	,CASE
		WHEN COALESCE(max(vr.targeted_users), (LISTAGG(DISTINCT r.targeted_users, ' & ') WITHIN GROUP (ORDER BY voucher_code))) = 'New Customers & Recurring Customers' THEN 'All Customers'
		WHEN COALESCE(max(vr.targeted_users), (LISTAGG(DISTINCT r.targeted_users, ' & ') WITHIN GROUP (ORDER BY voucher_code))) = 'Recurring Customers & New Customers' THEN 'All Customers'
		WHEN COALESCE(max(vr.targeted_users), (LISTAGG(DISTINCT r.targeted_users, ' & ') WITHIN GROUP (ORDER BY voucher_code))) LIKE '%All Customers%' THEN 'All Customers'
		ELSE COALESCE(max(vr.targeted_users),  max(r.targeted_users))
		END AS targeted_users
	,COALESCE(upper(max(vr.customer_type)), (LISTAGG(DISTINCT upper(r.customer_type), ' & ') WITHIN GROUP (ORDER BY voucher_code)))  AS customer_type
	,COALESCE(max(vr.country), (LISTAGG(DISTINCT upper(r.store_code), ' & ') WITHIN GROUP (ORDER BY voucher_code))) AS store_code
	,max(vr.items_total) AS item_number_limitation
	,max(vr.lowest_subscription_plan) AS subscription_plan_limitation
	,NULL AS product_limitation
	,count(CASE WHEN r.result = 'SUCCESS' AND r."object" = 'redemption' THEN 1 END) AS number_of_successful_redemptions
	,count(CASE WHEN r.result = 'FAILURE' AND r."object" = 'redemption' THEN 1 END) AS number_of_failed_redemptions
	,count(r.id) AS number_of_validations
	,count(CASE WHEN r.result = 'FAILURE' THEN 1 END) AS number_of_failed_validations
	,count(CASE WHEN r."object" = 'redemption_rollback' THEN 1 END) AS number_of_rolled_back_vouchers
FROM latest_vouchers v
LEFT JOIN check_voucher_code_pattern p ON v.code = p.code
LEFT JOIN validation_rules vr ON v.validation_rules_id = vr.val_rule_id
LEFT JOIN redemptions r ON v.code = r.v_code
GROUP BY 1,2;