   -----------Master Card_subscription
with card as(
select 
distinct customer_id,
first_value(first_event_timestamp) over (partition by customer_id order by first_event_timestamp asc
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as First_card_created_date
select DISTINCT  
s.subscription_id,
s.subscription_name,
s.order_id,
s.customer_id AS customer_id,
p.created_at AS user_created_date,
FIRST_VALUE (start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_subscription_start_date ,
min(CASE WHEN start_date>First_card_created_date THEN start_date END ) OVER (PARTITION BY c.customer_id) AS first_subscription_start_date_after_card,
coalesce(sa.first_asset_delivered,s.first_asset_delivery_date) AS first_asset_delivery_date,
s.start_date,
s.cancellation_date ,
c.First_card_created_date,
s.status,
s.country_name,
c2.customer_type,
s.subscription_plan ,
s.subscription_duration,
s.rank_subscriptions ,
s.subscriptions_per_customer,
s.rental_period ,
s.subscription_value ,
s.committed_sub_value ,
case when s.start_date>First_card_created_date then 1 else 0 end is_after_card,
case when FIRST_VALUE (start_date) OVER (PARTITION BY s.customer_id ORDER BY s.start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)>First_card_created_date then 1 else 0 end First_sub_after_card,
tu.first_timestamp_money_received,
tu.first_timestamp_payment_successful,
tu.first_timestamp_atm_withdrawal,
tu.first_timestamp_transaction_refund
from 
ods_production.subscription s 
left JOIN card c on c.customer_id=s.customer_id 
LEFT JOIN ods_production.customer c2 ON c2.customer_id =s.customer_id 
LEFT JOIN ods_production.subscription_assets sa ON s.subscription_id = sa.subscription_id
--WHERE c.user_id IS NOT NULL  ;



