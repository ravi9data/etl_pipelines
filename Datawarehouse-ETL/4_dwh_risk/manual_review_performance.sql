					drop table if exists dwh.manual_review_performance;
					create table dwh.manual_review_performance as
					select 
						s.subscription_id,
						s.status,
                        s.product_sku,
						s.cancellation_date,
                        s.cancellation_reason_churn,
						s.customer_id,
						s.order_id,
						md.reviewer,
						md."comment",
						md.reason,
						o.submitted_date,
                        s.product_name,
						--date of default,
						s.new_recurring,
						s.customer_type,
						cs.customer_label_new,
						c.clv,
						o.initial_scoring_decision,		
                        os.scoring_reason,
                        os.score,
						o.ordered_risk_labels,
                        o.ordered_sku,
                        o.shippingcity,
                        o.shippingcountry,
                        os.credit_limit,
                        c.paid_subscriptions,
						o.ordered_products,
						o.order_value,
                        s.outstanding_asset_value,
						last_valid_payment_category,
						next_due_date,
						outstanding_subscription_revenue,
						dpd
						from master.subscription s 
						left join ods_production.customer_scoring cs on s.customer_id = cs.customer_id 
						left join ods_data_sensitive.manual_review_decisions md on s.order_id = md.order_id
						left join ods_production."order" o on s.order_id = o.order_id
                        left join ods_production.order_scoring os on o.order_id = os.order_id
						left join master.customer c on c.customer_id = s.customer_id
						where start_date > '2020-08-01'
						and s.store_type = 'online'
						and o.manual_review_ends_at is not null ;

GRANT SELECT ON dwh.manual_review_performance TO tableau;
