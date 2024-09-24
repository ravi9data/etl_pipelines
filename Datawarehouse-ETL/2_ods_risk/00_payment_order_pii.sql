drop table if exists ods_data_sensitive.payment_order_pii;
create table ods_data_sensitive.payment_order_pii as
with adyen_info as(
SELECT 
		o.order_id,
        pr.psp_reference as payment_method_id,
        pr.merchant_reference,
        pr.card_type,
        pr.refusal_reason_raw,
        pr.card_number,
        pr.card_bin,
        pr.card_issuing_country,
        pr.card_issuing_bank,
        pr.funding_source,
        pr.cardholder_name,
        pr.shopper_email,
        NULL as paypal_email,
        'Adyen' as gateway_type,
        NULL as iban
        --pr.status_
	FROM 
	ods_data_sensitive.adyen_payment_requests pr 
	left join ods_production."order" o on o.payment_method_id_1 = pr.psp_reference  
	where pr.amount = 0
	and o.submitted_date is not null ),
ixopay_info as (
select 
	o.order_id,
	ip.uuid as payment_method_id,
	ip.merchant_txid as merchant_reference,
	ip.bin_brand as card_type,
	NULL as refusal_reason_raw,
	ip.last_four_digits as card_number,
	ip.first_six_digits as card_bin,
	ip.bin_country as card_issuing_country,
	bank_name as card_issuing_bank,
	ip.bin_type as funding_source,
	ip.card_holder as card_holder_name, 
	ip.shopper_email,
	ip.wallet_owner as paypal_email, 
	'Ixopay' as gateway_type,
	ip.iban as iban
	from ods_data_sensitive.ixopay_transactions ip 
	left join ods_production.order o on ip.uuid = o.payment_method_id_2
	where o.submitted_date is not null), 
	payment_info as (
	select * from adyen_info 
	union all 
	select * from ixopay_info),
orders_in_manual_review as ( 
 	select customer_id,count(*) as orders_in_MR from ods_production."order"
 	where status = 'MANUAL REVIEW' 
 	group by 1),
 ip_check as (	
 	select distinct ip_address_order as ip_address,count(distinct customer_id) as distinct_customers_with_ip 
	from ods_production."order" 
	where created_date>current_date-30
	group by 1),
  paypal_records as (
  select user_id,
		reference_id,
		json_extract_path_text(meta,'account_name') as paypal_name
		from stg_api_production.user_payment_methods
		where status = 'published'
		and payment_gateway_id = 1
  ),
order_details as 
          (
            select  oi.order_id as order_id_main,
            oi.product_name,
            oi.total_price,
            oi.quantity,
            oi.plan_duration,
            o.customer_id,
            o.total_orders,
          	o.ip_address_order,
            o.order_rank,
            o.status as order_status,
            o.created_date,
            o.submitted_date,
            o.payment_method,
            o.approved_date,
            o.manual_review_ends_at,
            o.store_type,
            o.billing_company__c,
            o.shipping_company__c,
            o.declined_reason,
            o.initial_scoring_decision,
            org.new_recurring,
            org.retention_group,
          	cs.customer_scoring_result,
            cs.fraud_type,
            c.customer_type,
            cp.phone_number,
            cp.first_name,
            cp.last_name,
          	cp.birthdate,
            cp.email,
            cp.paypal_email as customer_paypal_email,
          	o.billingstreet,
          	o.billingcity,
          	o.billingpostalcode,
          	o.billingcountry,
          	o.shippingstreet,
            o.shippingcity,
            o.shippingpostalcode,
          	o.shippingcountry,
          	o.shippingadditionalinfo,
          	o.voucher_code,
            cs.burgel_person_known,
            cs.burgel_score,
            cs.verita_person_known_at_address,
            cs.verita_score,
            cs.schufa_class,
            c.subscription_limit,
            sd.subscriptions,
            sd.subscription_revenue_due,
            sd.subscription_revenue_paid,
            sd.subscription_revenue_due -  sd.subscription_revenue_paid as outstanding_amount,
            sd.subscription_revenue_chargeback,
            sd.subscription_revenue_refunded,
            o.store_country,
            p.order_id,
            p.payment_method_id,
            p.merchant_reference,
            p.card_type,
            p.refusal_reason_raw,
            p.card_number,
            p.card_bin,
            p.card_issuing_country,
            p.card_issuing_bank,
            p.funding_source,
            p.cardholder_name,
            p.shopper_email,
            coalesce(p.paypal_email,pr.paypal_name) as paypal_email,
            p.gateway_type,
            p.iban,
            sc.score,
            sc.order_scoring_comments,
            case when o.shippingcity = o.billingcity then 'True'
	          	else 
		          case when fraud_type is not null then concat('False',concat('  ,  ',fraud_type))  
		           else	concat('False',' ,  No Fraud') end 
		    end as shipping_billing_city_match,
          	ip.distinct_customers_with_ip,
          	coalesce(omr.orders_in_mr,0) as customer_orders_in_mr,
          	case when payment_method not like '%paypal%'
	              then case when (((LOWER(cardholder_name) LIKE '%'||lower(first_name)||'%' ) and (LOWER(cardholder_name) LIKE  '%'||lower(last_name)||'%' )))  then 'Full match'
	              			when (((LOWER(cardholder_name) LIKE '%'||lower(first_name)||'%' ) and (LOWER(cardholder_name) NOT LIKE  '%'||lower(last_name)||'%' ))) OR 
	              			     (((LOWER(cardholder_name) NOT LIKE '%'||lower(first_name)||'%' ) and (LOWER(cardholder_name) LIKE  '%'||lower(last_name)||'%' ))) then 'Partial Match' 
	              			     else 'No match'		     
	              			     end
	              else case when lower(coalesce(p.paypal_email,customer_paypal_email)) = lower(email) then 'Full match'
		              when SPLIT_PART(p.paypal_email,'@',1) = SPLIT_PART(email,'@',1) then 'Partial Match'
	                   else 'No match'end 
	              end as payment_name_match,
	         case when payment_method not like '%paypal%'
		         then case when store_country = 'Germany' and card_issuing_country = 'DE' then 'Full match'
		          when store_country = 'Austria' and card_issuing_country = 'AT' then 'Full match'
			          when card_issuing_country is null then 'Not Available'
			          else 'Different country'end 
			          else 'Not Applicable'
				          end as card_country_match
      from ods_production.order_item oi 
    left join ods_production."order" o on oi.order_id = o.order_id
    left join ods_production.order_retention_group org on org.order_id = oi.order_id
    left join ods_production.customer c on o.customer_id = c.customer_id
    left join ods_production.customer_scoring cs on cs.customer_id = c.customer_id
    left join ods_production.customer_subscription_details sd on sd.customer_id = c.customer_id
    left join ods_data_sensitive.customer_pii cp on cp.customer_id = c.customer_id
    left join payment_info p on p.order_id = o.order_id
    left join ods_production.order_scoring sc on sc.order_id=o.order_id
    left join orders_in_manual_review omr on omr.customer_id = o.customer_id
    left join ip_check ip on ip.ip_address = o.ip_address_order
    left join paypal_records pr on pr.reference_id = o.payment_method_id_2
    )
    select *,
    '  Address & Fraud Type- '|| shipping_billing_city_match ||', Payment Match-  ' || payment_name_match || ',  Card Country-  ' || card_country_match as label_match_grouping
   from order_details o
    where o.submitted_date is not null
    and o.created_date>current_date-30;

GRANT SELECT ON ods_data_sensitive.payment_order_pii TO tableau;
