drop table if exists dwh.manual_review_historical;
create table dwh.manual_review_historical as

with oh  as (
	select * from master.order_historical where submitted_date::date = "date"
	and submitted_date is not null and customer_type = 'normal_customer'),
	c as (
	select * from master.customer_historical where customer_type ='normal_customer'
	) ,
	customer_labels as (--code to pull customer_label state at the time of order_submission only
	 SELECT customer_id,
       order_id,
       label_state,
       submitted_date,
       label_date
FROM   (
  SELECT o.*,
  		 clsc.label_state,
         clsc.updated_at AS label_date,
         ROW_NUMBER() OVER ( PARTITION BY o.customer_id  ORDER BY clsc.updated_at desc ) AS rn -- logic to get most recent label_Date  nearest to order_submitted date
  FROM   ods_production."order" o
        inner join s3_spectrum_rds_dwh_order_approval.customer_labels_state_changes clsc
         ON ( o.customer_id = clsc.customer_id AND clsc.updated_at <=  o.submitted_date)
)
WHERE  rn = 1
	),
	final_data as (
	select 
	distinct(o.order_id),--to remove dupicate records due to joins
	o.submitted_date ,
	cl.label_state as customer_label,
	cl.label_date,
	o.customer_id,
	c.customer_type,
	pod.related_order_ids,
	odmr.link_customer as sf_customer_link,
	o.ordered_products as product_names,
	c.active_subscriptions,
	os.similar_customers_count,
	os.top_similar_scores,
	odmr.snf_link,
	o.ordered_risk_labels as ordered_risk_labels,
	o.order_item_count,
	o.ordered_quantities as order_item_quantity,
	o.ordered_plan_durations as order_item_plan_duration,
	oh.basket_size as order_item_prices ,
	c.active_subscription_value, --historical
	c.subscription_revenue_due -  c.subscription_revenue_paid as outstanding_amount,
	c.subscription_limit - c.active_subscription_value - oh.basket_size as  overlimit_value,
	c.subscription_limit,
	c.subscription_revenue_paid as subscription_revenue_paid ,
	o.shippingcity as shipping_city,
	oh.voucher_code as voucher_code,
	oh.store_country,
	omr.mismatching_name, --check
    omr.mismatching_city, --check
    cp.first_name||' '||cp.last_name as full_name,
    op.cardholder_name, --check
    odmr.paypal_name,
    cp.email as customer_email, --not historical check
    op.shopper_email,-- check
    cp.paypal_email as customer_paypal_email, --historical
    op.paypal_email, --check
    css.paypal_verified,--check
    omr.is_foreign_card,--check
    omr.is_prepaid_card, --check
    omr.has_failed_first_payments,--check
    c.burgel_person_known, --historical
    c.burgel_score, --historical
    c.verita_person_known_at_address, --historical
    c.verita_score, --historical
    c.schufa_class, --historical
    os.score, --permanent confirmed
    c.failed_subscriptions as failed_recurring_payments, --historical
    oh.payment_method,--check
    op.card_type,--check
    op.card_issuing_country,--check
    op.card_issuing_bank,--check
	op.funding_source,--check
	css.accounts_cookie,
    css.web_users
    from ods_production."order" o
    left join customer_labels cl on cl.order_id = o.order_id 
    left join ods_data_sensitive.order_manual_review odmr on odmr.order_id = o.order_id 
    left join oh on o.order_id = oh.order_id and o.order_rank = oh.order_rank 
    left join c on ((o.submitted_date::Date = c."date") and (c.customer_id = o.customer_id))
    left join ods_data_sensitive.customer_pii cp on cp.customer_id = o.customer_id
    left join ods_production.order_scoring os on os.order_id = o.order_id
    left join ods_data_sensitive.order_payment_method op on op.order_id = o.order_id
    left join ods_production.order_manual_review_rules omr on omr.order_id = o.order_id
    left join ods_production.order_manual_review_previous_order_history pod on pod.main_order_id = o.order_id 
 	left join ods_production.customer_scoring css on css.customer_id = o.customer_id
                    where o.submitted_date is not null and c.customer_type = 'normal_customer'
                    )
    select * from final_data;  
	
grant select on dwh.manual_review_historical to group risk_manual_review;	