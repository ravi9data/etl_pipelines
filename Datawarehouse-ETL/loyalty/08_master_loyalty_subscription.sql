drop table if exists master_referral.loyalty_subscription;
create table master_referral.loyalty_subscription AS
with loyalty_customer as(
SELECT customer_id,
		country,
		first_event_name,
		customer_type,
	 first_referral_event,
	 first_subscription_start_date,
	 is_card_user
FROM master_referral.loyalty_customer
WHERE first_event_name IS NOT NULL 
)
select DISTINCT  
s.subscription_id,
s.order_id,
--s.subscription_sf_id,
s.customer_id AS customer_id,
c.first_subscription_start_date ,
min(CASE WHEN start_date::date>=First_referral_event::date THEN start_date END ) OVER (PARTITION BY c.customer_id) AS first_subscription_start_date_after_referral,
s.start_date,
s.cancellation_date ,
c.First_referral_event,
c.first_event_name,
s.status,
COALESCE(c.country,s.country_name) AS country,
cc.customer_type,
s.subscription_plan ,
s.subscription_duration,
s.rank_subscriptions ,
s.subscriptions_per_customer,
s.rental_period ,
s.subscription_value ,
s.committed_sub_value ,
case when s.start_date>First_referral_event AND first_referral_event IS NOT NULL  then 1 else 0 end is_after_referral,
CASE WHEN s.start_date>First_referral_event AND first_event_name='referral_guest' THEN 'referal_guest' ELSE 'referral_host' END referral_classification,
case when c.first_subscription_start_date>First_referral_event then 1 else 0 end First_sub_after_referral,
CASE WHEN c.customer_id IS NULL THEN 'Non_referral_user' ELSE
	CASE WHEN s.start_date>First_referral_event AND first_event_name='referral_guest' THEN 'referral_guest' ELSE 'referral_host' END
 END AS User_classification,
CASE WHEN is_card_user = TRUE THEN TRUE ELSE FALSE END AS is_card_user
from 
ods_production.subscription s 
left JOIN loyalty_customer c on c.customer_id=s.customer_id
LEFT JOIN ods_production.customer cc ON cc.customer_id=s.customer_id
WHERE cc.customer_type='normal_customer';

GRANT SELECT ON master_referral.loyalty_subscription TO tableau;
