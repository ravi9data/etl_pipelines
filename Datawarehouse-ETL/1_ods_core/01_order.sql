--BILLING INFO ON ORDER LEVEL - NEW INFRA
drop table if exists stg_scoring_customer_fraud_check_completed;
drop table if exists stg_internal_order_cancelled;
drop table if exists stg_internal_order_placed_v2;
drop table if exists stg_internal_billing_payments_v2;
drop table if exists stg_internal_order_placed;
drop table if exists stg_internal_risk_order_decisions;
create temp table stg_scoring_customer_fraud_check_completed as select * from stg_kafka_events_full.stream_scoring_customer_fraud_check_completed;
create temp table stg_internal_order_cancelled as select * from stg_kafka_events_full.stream_internal_order_cancelled;
create temp table stg_internal_order_placed_v2 as select * from stg_kafka_events_full.stream_internal_order_placed_v2;
create temp table stg_internal_order_placed as select * from stg_kafka_events_full.stream_internal_order_placed;
create temp table stg_internal_risk_order_decisions as select * from stg_kafka_events_full.stream_internal_risk_order_decisions_v3;

----NEW INFRA ODS ORDER

DROP TABLE IF EXISTS stg_kafka_order;
	CREATE TEMP TABLE stg_kafka_order
	SORTKEY(order_id)
	DISTKEY(order_id)
	as
with cte_order_placed_v2 as--using the new order placed tabke in case they decommissioned order placed v1 table
	(
	select
		 event_timestamp
		,(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'total'), 'in_cents'))::INTEGER total_in_cents
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'store'), 'country_id') country_id
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'store'), 'store_id') store_id
		,JSON_EXTRACT_PATH_text(payload,'order_number') order_number
		,JSON_EXTRACT_PATH_text(payload,'order_mode') order_mode
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'user'), 'user_id') user_id
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'payment_method'), 'billing_account_id') billing_account_id
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'payment_method'), 'source_type') billing_source_type
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'address1') billing_address1
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'address2') billing_address2
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'city') billing_city
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'country') billing_country
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'zipcode') billing_zipcode
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'address1') shipping_address1
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'address2') shipping_address2
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'city') shipping_city
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'country') shipping_country
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'zipcode') shipping_zipcode
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'additional_info') shipping_additional_info
		,row_number() over (partition by order_number order by event_timestamp asc) as idx
		,'' as line_items_json
		,'' as order_item_id
	from stg_internal_order_placed_v2 q_
	)
	,cte_old_mix as--using the order placed v1 table to get the old mix orders before june 2021
	(
	select
		event_timestamp,
		total_in_cents,
		country_id,
		store_id,
		order_number,
		order_mode,
		user_id,
		billing_account_id,
		source_type AS billing_source_type,
		billing_address1,
		billing_address2,
		billing_city,
		billing_country,
		billing_zipcode,
		shipping_address1,
		shipping_address2,
		shipping_city,
		shipping_country,
		shipping_zipcode,
		shipping_additional_info
		,row_number() over (partition by order_number order by event_timestamp asc) as idx
		,json_extract_array_element_text(line_items,0, true) as line_items_json
		,JSON_EXTRACT_PATH_text(line_items_json,'line_item_id') order_item_id
	from stg_internal_order_placed siop
	where order_mode = 'MIX'
	and order_item_id = 0
	)
	,order_placed_tmp as
	(
	select * from cte_old_mix where idx = 1 and order_number not in (select distinct  order_number from cte_order_placed_v2)
	union all
	select * from cte_order_placed_v2 where idx = 1
	)
	,order_placed as
	(
	select
		 *
		,to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as event_time
	from order_placed_tmp
	)
	,order_decision as
	(
	select
		 q_.*
	    ,row_number() over (partition by order_number order by event_timestamp desc)  as idx
	from stg_internal_risk_order_decisions q_
	)
	,grouping_risk as
	(
	select distinct *
	from order_decision
	where idx = 1
	)
	,approved as
	(
	select
		order_number as order_id,
		decision,
		decision_message,
		to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as decision_date,
		case when decision = 'decline' then decision_message else null end as declined_reason,
		case when decision = 'decline' then decision_date end as declined_date,
		case when decision = 'approve' then decision_date end as approved_date
	from grouping_risk
	)
	,order_item_aggregated as
	(
    select DISTINCT
		 order_id,
		 avg(plan_duration) as avg_plan_duration,
		 listagg(plan_duration,', ') as plan_duration,
		 listagg(product_sku,', ') as skus,
		 listagg(product_name,', ') as products,
		 listagg(subcategory_name,', ') as subcategories,
		 listagg(brand,', ') as brands,
		 listagg(category_name,', ') as categories,
		 listagg(price,', ') as prices,
		 listagg(quantity,', ') as quantities,
		 listagg(risk_label,', ') as risk_labels,
		 sum(quantity) as order_item_count,
		 sum(quantity * price) as basket_size, --in new infra this represents actual item value in order, total price is considering voucher_discount
		 max(case when trial_days >=1 then 1 else 0 end) as is_trial_order
    from ods_production.order_item
    group by 1
    )
    ,fraud_check_output as
	(
    select
		distinct
        event_timestamp,
        json_extract_path_text(payload, 'user_id') as user_id,
        json_extract_path_text(payload, 'order_number') as order_number,
		json_extract_path_text(payload, 'decision') as fraud_check_decision,
        (timestamptz 'epoch' + (JSON_EXTRACT_PATH_text(payload, 'completed_at'))::int * interval '1 second') as completed_at,
        ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp desc) AS idx
    from stg_scoring_customer_fraud_check_completed
	)
    ,fd_final as
	(
	select *
	from fraud_check_output
	where idx = 1
	)
	,cancelled_ as
	(
	select
		distinct event_timestamp as canceled_date,
		event_name,
		JSON_EXTRACT_PATH_text(payload,'order_number') as order_number,
		JSON_EXTRACT_PATH_text(payload,'user_id') as user_id,
		INITCAP(REPLACE((JSON_EXTRACT_PATH_text(payload,'reason')),'_',' ')) as reason,
		ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp desc) AS rank_cancelled_orders
	from stg_internal_order_cancelled
	)
	,cancelled_final_ as
	(
	select *
	from cancelled_
	where rank_cancelled_orders = 1
	)
	, delivered_contracts AS -- TO make up FOR incomplete activation events (= delivery / activation of billing cycle) IN the contracts SOURCE
	(
	SELECT DISTINCT subscription_id, order_id, as2.delivered_at
	FROM ods_operations.allocation_shipment as2 
	WHERE delivered_at IS NOT NULL
		AND failed_delivery_at IS NULL 
	)
	, cancellations_from_contracts AS 
	(
	SELECT
		contract_id, 
		order_number AS order_id,
		event_name,
		kafka_received_at::TIMESTAMP AS event_timestamp,
		terminated_at,
		termination_reason AS cancellation_reason,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC) AS rowno
	FROM staging.customers_contracts
	UNION
	SELECT
		subscription_id AS contract_id,
		order_id,
		'delivered' AS event_name,
		delivered_at::TIMESTAMP AS event_timestamp,
		NULL AS terminated_at,
		NULL AS cancellation_reason,
		NULL AS rowno
	FROM delivered_contracts
	)
	, cancellations_from_contracts_cleaned AS 
	(
	SELECT 
		cfc1.order_id,
		MAX(CASE WHEN cfc1.event_name = 'cancelled' THEN to_timestamp(cfc1.terminated_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z') END) AS order_cancellation_timestamp,
		MAX(NULLIF(cfc2.cancellation_reason, ' ')) AS cancellation_reason_order, -- Remove EMPTY strings.
		COUNT(DISTINCT cfc1.contract_id) AS num_subscriptions,
		COUNT(DISTINCT CASE WHEN cfc1.event_name = 'activated' THEN cfc1.contract_id END) AS num_activated_subscriptions,
		COUNT(DISTINCT CASE WHEN cfc1.event_name = 'delivered' THEN cfc1.contract_id END) AS num_delivered_subscriptions,
		COUNT(DISTINCT CASE WHEN cfc1.event_name = 'cancelled' THEN cfc1.contract_id END) AS num_cancelled_subscriptions
	FROM cancellations_from_contracts cfc1
	LEFT JOIN cancellations_from_contracts cfc2
	  ON cfc1.contract_id = cfc2.contract_id
	  AND cfc1.event_timestamp = cfc2.event_timestamp
	  AND cfc2.cancellation_reason IS NOT NULL 
	  AND cfc2.rowno = 1
	GROUP BY 1
	HAVING 
		num_subscriptions = num_cancelled_subscriptions -- ORDER cancellation DEFINED AS ALL subs WITHIN ORDER cancelled
		AND num_delivered_subscriptions = 0             -- BEFORE ANY OF them were activated (need to establish a boundary condition here)
		AND num_activated_subscriptions = 0					-- OR delivered (to account for missing activation (= delivery) events )
		AND cancellation_reason_order NOT IN 
			('damage_claim', 'end_of_cycle_return', 'replacement_within_14_days', 'revocation')  -- could ONLY exist IF delivered (catering for missing delivery events)
	)
	, manual_review_orders AS 
	(
	SELECT order_id
	FROM stg_curated.risk_eu_order_decision_intermediate_v1
	WHERE outcome_namespace = 'MANUAL_REVIEW'
	UNION
	SELECT order_id
	FROM s3_spectrum_kafka_topics_raw_sensitive.risk_us_order_decision_intermediate_v1
	WHERE outcome_namespace = 'MANUAL_REVIEW'
	)
	,payment_method as
	(
	select
		billing_account_id,
		merchant_transaction_id,
		reference_id,
		payment_gateway_id,
		case when payment_gateway_id = 1 then  'paypal-gateway'
			when payment_gateway_id = 2 then 'dalenys-bankcard-gateway'
				when payment_gateway_id = 3 then 'sepa-gateway'
					when payment_gateway_id = 9 then 'bankcard-gateway'
						end as payment_method
	from stg_api_production.user_payment_methods
	)
	,voucher_pre as
	(
	select distinct
		json_extract_path_text(payload, 'order_number') as order_number,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'code'),'') as voucher_code,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'type'),'') as voucher_type,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'effect'), '') as voucher_effect,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'percent_off'),'') as voucher_percent_off,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount_amount'),'in_cents'),'') as voucher_amount_in_cents,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount_amount'),'currency'),'') as voucher_currency,
		NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'metadata'),'recurring'),'') as is_voucher_recurring,
		ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp DESC) AS idx
	from stg_internal_order_placed_v2
	)
	,voucher_final as
	(
	select
		order_number,
		voucher_type,
		voucher_code,
		voucher_percent_off as voucher_value,
		(voucher_amount_in_cents::decimal/100)::decimal(10,2) as voucher_discount,
		voucher_currency,
		case when is_voucher_recurring = 'true' then true
			when is_voucher_recurring = 'false' then false
				else null end as is_voucher_recurring
	from voucher_pre
	where idx = 1
	)
	select distinct
	op.order_number as order_id,
	op.user_id::int as customer_id,
	1::int as is_user_logged_in,
	op.store_id as store_id,
	to_timestamp (
		CASE WHEN tb2.created_date IS NOT NULL AND op.event_time::timestamp <= tb2.created_date::timestamp THEN op.event_time::timestamp
			 WHEN tb2.created_date IS NOT NULL THEN tb2.created_date::timestamp
			 ELSE op.event_time::timestamp END
			, 'yyyy-mm-dd HH24:MI:SS') as created_date,
	to_timestamp (op.event_time, 'yyyy-mm-dd HH24:MI:SS') as submitted_date,
	NULL::timestamptz as manual_review_ends_at,
	to_timestamp (bp.paid_date, 'yyyy-mm-dd HH24:MI:SS') as paid_date ,
	NULL::timestamptz  as updated_date,
	af.approved_date,
	first_charge_date::timestamptz,
	COALESCE(cn.canceled_date::timestamptz, cfcc.order_cancellation_timestamp::timestamptz) as canceled_date,
	op.billing_address1 + op.billing_address2 as billingstreet,
	op.billing_city as billingcity,
	op.billing_country as billingcountry,
	op.billing_zipcode as billingpostalcode,
	op.shipping_address1 + op.shipping_address2 as shippingstreet,
	op.shipping_city as shippingcity,
	op.shipping_country as shippingcountry,
	op.shipping_zipcode as shippingpostalcode,
	op.shipping_additional_info as shippingadditionalinfo,
	NULL as billing_company__c,
	NULL as shipping_company__c,
	CASE WHEN op.billing_source_type ILIKE '%PayByInvoice%' THEN 'pay-by-invoice' ELSE  payment_method END AS payment_method,
	merchant_transaction_id as payment_method_id_1,
	reference_id as payment_method_id_2,
	case when bp.payment_method_type = 'pay_by_invoice' then true else false end as is_pay_by_invoice,
	min(CASE WHEN paid_date is not null
			THEN to_timestamp (op.event_time, 'yyyy-mm-dd HH24:MI:SS')
			ELSE NULL::timestamp without time zone END) OVER (PARTITION BY op.user_id::int) as acquisition_date_temp,
	case when cn.event_name ='cancelled' then 'CANCELLED'
		WHEN cfcc.order_id IS NOT NULL THEN 'CANCELLED'
		when paid_date is not null then 'PAID'
		when bp.event_name = 'failed' then 'FAILED FIRST PAYMENT'
		when af.decision = 'approve' then 'APPROVED'
		when af.decision = 'decline' then 'DECLINED'
		when mro.order_id is not null then 'MANUAL REVIEW'
		when fd.fraud_check_decision = 'manual_review' then 'FRAUD CHECK'
		else 'PENDING APPROVAL' end  as status,
	'complete' as step,
	rank() OVER (PARTITION BY op.user_id ORDER BY to_timestamp (op.event_time, 'yyyy-mm-dd HH24:MI:SS')) as order_rank_temp,
	--NULL::bigint as total_orders, (Replaced in final union)
	COALESCE(cn.reason, initcap(REPLACE(cfcc.cancellation_reason_order, '_', ' '))) as cancellation_reason,
	declined_reason,
	(op.total_in_cents::decimal/100)::double precision + COALESCE(vf.voucher_discount::double precision,0) as order_value,
	p.order_item_count::bigint,
		p.basket_size,
		p.is_trial_order,
		p.avg_plan_duration,
		p.categories as ordered_product_category,
		p.skus as ordered_sku,
		p.prices as ordered_prices,
		p.quantities as ordered_quantities,
		p.products as ordered_products,
		p.plan_duration as ordered_plan_durations,
		p.risk_labels as ordered_risk_labels,
	vf.voucher_code as voucher_code,
	case when vf.voucher_code like '%SBUSA%' then 'Student Beans USA'
		 when vf.voucher_code is null then 'no voucher'
	else 'other vouchers' end as is_special_voucher,
	vf.voucher_type as voucher_type,
	Vf.voucher_value as voucher_value,
	vf.voucher_discount::double precision as voucher_discount,
	vf.is_voucher_recurring,
	false as is_in_salesforce,
	st.store_type,
	case
		 when u.user_type='business_customer' then 'B2B'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
		 when st.store_short in ('Partners Online','Partners Offline') then 'Partnerships'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
		 else 'Grover'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
		end as store_commercial,
	 st.country_name as store_country,
	 case when lower(decision) like '%approve%' then 'APPROVED'
		 when lower(decision) like '%decline%' then 'DECLINED'
			 else null end as initial_scoring_decision,
	NULL as ip_address_order,
	NULL::bigint as payment_method_pp,
	NULL::bigint as payment_method_sepa,
	NULL::bigint as payment_method_dalenys,
	op.order_mode  as order_mode
	from order_placed op
	left join payment_method pm
		on op.billing_account_id = pm.billing_account_id
	left join ods_production.billing_payments_final bp
	   on bp.order_number = op.order_number
	left join fd_final fd on
		fd.order_number = op.order_number
	left join approved af
	   on af.order_id = op.order_number
	left join cancelled_final_ cn on
		cn.order_number = op.order_number
	left join voucher_final vf on
	   vf.order_number = op.order_number
	 left join order_item_aggregated p
	  on p.order_id = op.order_number
	left join ods_production.store st
	 on st.id = op.store_id
	 left join stg_api_production.spree_users u
	 on u.id=op.user_id
	 LEFT JOIN stg_curated.checkout_eu_us_cart_orders_updated_v1 tb2
	 	ON op.order_number = tb2.order_id
	 LEFT JOIN manual_review_orders mro
	 	ON mro.order_id = op.order_number
	 LEFT JOIN cancellations_from_contracts_cleaned cfcc
	 	ON cfcc.order_id = op.order_number
	;

--ODS ORDER LEGACY INFRA
DROP TABLE IF EXISTS ods_order_legacy;
    CREATE TEMP TABLE ods_order_legacy
	SORTKEY(order_id)
	DISTKEY(order_id)
	as
with order_item_aggregated as (
    select DISTINCT
     order_id,
     avg(plan_duration) as avg_plan_duration,
     listagg(plan_duration,', ') as plan_duration,
     listagg(product_sku,', ') as skus,
     listagg(product_name,', ') as products,
     listagg(subcategory_name,', ') as subcategories,
     listagg(brand,', ') as brands,
     listagg(category_name,', ') as categories,
     listagg(total_price,', ') as prices,
     listagg(quantity,', ') as quantities,
     listagg(risk_label,', ') as risk_labels,
     sum(quantity) as order_item_count,
 	 sum(total_price) as basket_size,
 	 max(case when trial_days >=1 then 1 else 0 end) as is_trial_order
    from ods_production.order_item
    group by 1
    )
    ,paid_Date as (
   select
    order_id, max(start_date) as paid_date
    from ods_production.subscription
    group by 1)
, pm as (
select distinct
o.user_id as customer_id,
o."number" as order_id,
LAST_VALUE(merchant_transaction_id) over (partition by o.user_id ,o."number" order by upm.created_at asc
rows between unbounded preceding and unbounded following) as payment_method_last,
count(merchant_transaction_id) over (partition by o.user_id ,o."number") as payment_method_count,
count(case when merchant_transaction_id like ('%paypal%') then merchant_transaction_id end)  over (partition by o.user_id ,o."number") as payment_method_pp,
count(case when merchant_transaction_id like ('%sepa%') then merchant_transaction_id end)  over (partition by o.user_id ,o."number") as payment_method_sepa,
count(case when merchant_transaction_id like ('%dalenys%') then merchant_transaction_id end)  over (partition by o.user_id ,o."number") as payment_method_dalenys
from stg_api_production.user_payment_methods upm
inner join stg_api_production.spree_orders o
 on o.user_id=upm.user_id
)
 ,payment as (   select
order__c as order_id,
min(least(date_due__c,date_failed__c,date_paid__c)) as first_charge_date
from stg_salesforce.subscription_payment__c
group by 1)
, ref as (
select distinct customer_id_mapped as customer_id
from traffic.page_views v
where v.page_url like '%/join%'
and user_registration_date::date <= v.page_view_start::date+30
and user_registration_date::date >= v.page_view_start::date)

select distinct
	a."number" as order_id,
	coalesce(
       case when s.spree_customer_id__c::VARCHAR=' '
        then null else s.spree_customer_id__c::VARCHAR
       end,a.user_id::VARCHAR)::integer as customer_id,
    case
     when coalesce(
       		case when s.spree_customer_id__c::VARCHAR=' '
             then null else s.spree_customer_id__c::VARCHAR
       end,a.user_id::VARCHAR)::integer is not null
      then 1
       else 0 end as is_user_logged_in,
	coalesce(s.store_id__c,a.store_id)::VARCHAR as store_id,
	a.created_at as created_date,
    s.createddate as submitted_date,
    s.manual_review_ends_at__c as manual_review_ends_at,
    d.paid_date,
	greatest(a.updated_at,s.lastmodifieddate,s.systemmodstamp) as updated_date,
    coalesce(s.state_approved__c,a.approved_at) as approved_date,
    payment.first_charge_date,
	coalesce(s.state_cancelled__c,a.canceled_at) as canceled_date,
    s.billingstreet,
    s.billingcity,
    s.billingcountry,
    s.billingpostalcode,
    s.shippingstreet,
    s.shippingcity,
    s.shippingcountry,
    s.shippingpostalcode,
    s.shipping_additional_info__c as shippingadditionalinfo,
   	s.billing_company__c,
   	s.shipping_company__c,
    coalesce(s.payment_method_name__c,
      (case
     when pm2.merchant_transaction_id like ('%sepa%') then 'sepa-gateway'
     when pm2.merchant_transaction_id like ('%paypal%') then 'paypal-gateway'
     when pm2.merchant_transaction_id like ('%dalenys%') then 'dalenys-bankcard-gateway'
    end),
    (case
     when pm.payment_method_last like ('%sepa%') then 'sepa-gateway'
     when payment_method_last like ('%paypal%') then 'paypal-gateway'
     when payment_method_last like ('%dalenys%') then 'dalenys-bankcard-gateway'
    end),'n/a')
     as payment_method,
    s.payment_method_id_1__c as payment_method_id_1,
    s.payment_method_id_2__c as payment_method_id_2,
     false as is_pay_by_invoice,
	min(
        CASE
           WHEN upper(coalesce(s.status,a.state)::text) = 'PAID'::text THEN a.created_at
               ELSE NULL::timestamp without time zone
         			END) OVER (PARTITION BY a.user_id) as acquisition_date_temp,
case 
	when replace(upper(coalesce(s.status,a.state)),'_',' ') in ('CANCELLED','CANCELED') then 'CANCELLED'
	when s.status = 'MANUAL REVIEW' and a.state = 'declined' then 'DECLINED'
else replace(upper(coalesce(s.status,a.state)),'_',' ') end as status,
a.step as step,
	rank() OVER (PARTITION BY a.user_id ORDER BY a.created_at) as order_rank_temp,
    --count(*) OVER (PARTITION BY a.user_id) AS total_orders,  (Replaced in final union)
	case
		when upper(coalesce(s.status,a.state))='CANCELLED' then s.reason__c
			else null end as cancellation_reason,
	case
		when upper(coalesce(s.status,a.state))='DECLINED'
        or (upper(s.scoring_decision__c) = 'DECLINED' AND upper(s.status)='MANUAL REVIEW')
        then s.reason__c
	else null end as declined_reason,
    a.item_total as order_value,
    p.order_item_count,
    p.basket_size,
    p.is_trial_order,
    p.avg_plan_duration,
    p.categories as ordered_product_category,
    p.skus as ordered_sku,
    p.prices as ordered_prices,
    p.quantities as ordered_quantities,
    p.products as ordered_products,
    p.plan_duration as ordered_plan_durations,
    p.risk_labels as ordered_risk_labels,
--	a.voucherify_coupon_code as voucher_code,
--	a.voucherify_coupon_type as voucher_type,
--	a.voucherify_coupon_value as voucher_value,
--    a.voucherify_discount_total as voucher_discount,
    case
      when s.voucher__c in (select voucher_code from ods_production.order_voucher_mapping
                             where voucher_category='voucherify_coupon_code')
          then 'G-'||coalesce(s.voucher__c,a.voucherify_coupon_code)
else coalesce(s.voucher__c,a.voucherify_coupon_code) end as voucher_code,
	case
  when voucher_code is not null and  voucher_code not LIKE ('G-%') and voucher_code not LIKE ('R-%')  and ref.customer_id is not null
  then 'Referrals acquired - Other vouchers'
  when s.voucher__c in (select voucher_code from ods_production.order_voucher_mapping
                             where voucher_category='voucher')
  then s.voucher__c
   when lower(voucher_code) in ('blackcat')
   then lower(voucher_code)
  when s.voucher__c in (select voucher_code from ods_production.order_voucher_mapping
                             where voucher_category='refer_friend_voucher')
OR voucher_code LIKE ('G-%') THEN 'Referals: Refer a friend Voucher'
when coalesce(voucher_code,'') LIKE 'R-%' then'Referals: Reward Voucher'
when coalesce(voucher_code,'') LIKE 'N26-%' then'N26 Voucher'
when coalesce(voucher_code,'') = '' and ref.customer_id is not null
then 'no voucher - Referrals acquired'
when coalesce(voucher_code,'') = ''
then 'no voucher'
  else 'other vouchers'
 end as is_special_voucher,
	s.voucherify_coupon_type__c as voucher_type,
	s.voucherify_coupon_value__c as voucher_value,
    CASE
     WHEN a.created_at::date < '2019-12-05'
      AND COALESCE(s.voucherify_discount__c,0) > COALESCE(a.item_total,0) + COALESCE(s.amount_shipment__c, 0)
      THEN COALESCE(a.item_total,0)+ COALESCE(s.amount_shipment__c, 0)
     WHEN  a.created_at::date >= '2019-12-05'
      AND COALESCE(s.voucherify_discount__c,0) > COALESCE(a.item_total,0)
      THEN a.item_total
     ELSE s.voucherify_discount__c
    END AS voucher_discount,
    coupon_recurrent as is_voucher_recurring,
    case when a."number"=s.spree_order_number__c
     or upper(coalesce(s.status,a.state)::text) in ('DECLINED','APPROVED')
      then true else false end as is_in_salesforce,
    st.store_type,
   case
     when u.user_type='business_customer' then 'B2B'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
     when st.store_short in ('Partners Online','Partners Offline') then 'Partnerships'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
     else 'Grover'||' '||case when st.country_name='Germany' then st.country_name else 'International' end
    end as store_commercial,
    st.country_name as store_country,
    s.scoring_decision__c as initial_scoring_decision,
    s.ip_address__c as ip_address_order,
    least(pm.payment_method_pp,1) as payment_method_pp,
    least(pm.payment_method_sepa,1) as payment_method_sepa,
    least(pm.payment_method_dalenys,1) as payment_method_dalenys,
     'LEGACY' as order_mode
from stg_api_production.spree_orders a
left join stg_api_production.spree_users u
 on u.id=a.user_id
left join stg_salesforce."order" s
	on a."number"=s.spree_order_number__c
left join ods_production.store st
 on coalesce(s.store_id__c,a.store_id)::VARCHAR=st.id
left join order_item_aggregated p
 on a."number"=p.order_id
left join pm
 on pm.order_id=a."number"
left join stg_api_production.user_payment_methods pm2
 on pm2.id=a.current_payment_source_id
left join paid_date d
 on d.order_id=a."number"
left join payment on
 payment.order_id=s."id"
left join ref on ref.customer_id = s.spree_customer_id__c
;


--CART ORDERS
DROP TABLE IF EXISTS cart_orders;
    CREATE TEMP TABLE cart_orders
	SORTKEY(order_id)
	DISTKEY(order_id)
	AS
	SELECT
		tb2.order_id AS order_id,
		split_part(tb2.customer_id , '.', 1) ::INT AS customer_id,
		CASE WHEN tb2.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_user_logged_in,
		st.id::VARCHAR AS store_id,
		to_timestamp (tb2.created_date, 'yyyy-mm-dd HH24:MI:SS') AS created_date,
		NULL::timestamp AS submitted_date,
		NULL::timestamp AS manual_review_ends_at,
		NULL::timestamp AS paid_date,
		to_timestamp (tb2.updated_date, 'yyyy-mm-dd HH24:MI:SS') AS updated_date,
		NULL::timestamp AS approved_date,
		NULL::timestamp AS first_charge_date,
		NULL::timestamp AS canceled_date,
		NULL::varchar AS billingstreet,
		NULL::varchar AS billingcity,
		NULL::varchar AS billingcountry,
		NULL::varchar AS billingpostalcode,
		NULL::varchar AS shippingstreet,
		NULL::varchar AS shippingcity,
		NULL::varchar AS shippingcountry,
		NULL::varchar AS shippingpostalcode,
		NULL::varchar AS shippingadditionalinfo,
		NULL::varchar AS billing_company__c,
		NULL::varchar AS shipping_company__c,
		NULL::varchar AS payment_method,
		tb2.payment_method_id::varchar AS payment_method_id_1,
		NULL::varchar AS payment_method_id_2,
		FALSE AS is_pay_by_invoice,
		NULL::timestamp without time zone AS acquisition_date_temp, 
		UPPER(tb2.status) AS status,
		NULL::varchar AS step, 
		NULL::int AS order_rank_temp, 
		NULL::varchar AS cancellation_reason,
		NULL::varchar AS declined_reason,
		COALESCE(JSON_EXTRACT_PATH_text(tb2.order_value,'in_cents')::float/100 , SUM(tb2.total_amount::float) ) AS order_value,
		SUM(tb2.quantity::int) AS order_item_count,
		COALESCE(JSON_EXTRACT_PATH_text(tb2.basket_size,'in_cents')::float/100 , SUM(tb2.total_amount::float) )  AS basket_size,
		NULL::int AS is_trial_order,
		AVG(tb2.committed_months::float) AS avg_plan_duration,
		LISTAGG(DISTINCT p.category_name, ', ') AS ordered_product_category,
		LISTAGG(DISTINCT tb2.product_sku, ', ') AS ordered_sku,
		LISTAGG((tb2.price::decimal(10,2))::varchar, ', ') AS ordered_prices,
		LISTAGG(tb2.quantity, ', ') AS ordered_quantities,
		LISTAGG(DISTINCT p.product_name, ', ') AS ordered_products,
		LISTAGG(tb2.committed_months, ', ') AS ordered_plan_durations,
		NULL::varchar AS ordered_risk_labels,
		NULL::varchar AS voucher_code,
		'no voucher'::varchar AS is_special_voucher,
		NULL::varchar AS voucher_type,
		NULL::varchar AS voucher_value,
		NULL::float AS voucher_discount,
		NULL::boolean AS is_voucher_recurring,
		FALSE AS is_in_salesforce,
		st.store_type AS store_type,
		CASE
			WHEN u.user_type='business_customer' THEN 'B2B'||' '||
				CASE WHEN st.country_name='Germany' THEN st.country_name ELSE 'International' END
			WHEN st.store_short IN ('Partners Online','Partners Offline') THEN 'Partnerships'||' '||
				CASE WHEN st.country_name='Germany' THEN st.country_name ELSE 'International' END
			ELSE 'Grover'||' '||
				CASE WHEN st.country_name='Germany' THEN st.country_name ELSE 'International' END
			END AS store_commercial,
		st.country_name AS store_country,
		NULL::varchar AS initial_scoring_decision,
		customer_ip_address::varchar AS ip_address_order,		
		NULL::int AS payment_method_pp,
		NULL::int AS payment_method_sepa,
		NULL::int AS payment_method_dalenys,
		'CART'::varchar AS order_mode
FROM stg_curated.checkout_eu_us_cart_orders_updated_v1 tb2
	LEFT JOIN ods_production.product p
		ON tb2.product_sku = p.product_sku
	LEFT JOIN ods_production.store st
		ON tb2.store_code = st.store_code
	LEFT JOIN stg_api_production.spree_users u
		ON tb2.customer_id = u.id
	LEFT JOIN ods_order_legacy odl 
		ON tb2.order_id = odl.order_id
	LEFT JOIN stg_kafka_order sko 
		ON tb2.order_id = sko.order_id
	WHERE sko.order_id IS NULL AND odl.order_id IS NULL -- IN ORDER TO DELETE the orders we already have
	GROUP BY 1,2,3,4,5,9,25,27,29,47,52,53,54,55,57,61, tb2.order_value, tb2.basket_size
;



DROP TABLE IF EXISTS order_final;
CREATE TEMP TABLE order_final
	as
with union_order as
(
select * from ods_order_legacy where order_id not in (select order_id from stg_kafka_order)
union all
select * from stg_kafka_order
UNION all
SELECT * FROM cart_orders where order_id not in (select order_id from stg_kafka_order)
)
select
*,
min(acquisition_date_temp) over (partition by customer_id) as acquisition_date,
CASE WHEN customer_id IS NOT NULL THEN rank() OVER (PARTITION BY customer_id ORDER BY created_date) ELSE 1 END as order_rank,
CASE WHEN customer_id IS NOT NULL THEN count(*) over (partition by customer_id) ELSE 1 END as total_orders
from union_order;

Alter table order_final drop column acquisition_date_temp;
Alter table order_final drop column order_rank_temp;

begin;

truncate table ods_production.order;

insert into ods_production.order
select * from order_final a
where order_mode != 'SWAP'
	/*exclude datadog users
		SELECT distinct user_id::int AS customer_id
		FROM atomic.events 
		WHERE useragent like '%Datadog%'
		AND user_id is not null
	*/
  and coalesce(a.customer_id,0) not in (1550695,1192749);

drop table if exists stg_scoring_customer_fraud_check_completed;
drop table if exists stg_internal_order_cancelled;
drop table if exists stg_internal_order_placed_v2;
drop table if exists stg_internal_order_placed;
drop table if exists stg_internal_risk_order_decisions;

commit;
