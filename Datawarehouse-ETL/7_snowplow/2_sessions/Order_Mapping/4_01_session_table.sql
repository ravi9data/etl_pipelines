drop table if exists web.session_conversions;
create table web.session_conversions as 
select 
s.session_id,
max(case when isnumeric(s.customer_id::text) then s.customer_id::integer
		 when isnumeric(om.customer_id_web::text) then om.customer_id_web::integer
		 when om.customer_id_order then om.customer_id_order
		 else null
	end) as customer_id,
max(case when cart_date is not null and last_touchpoint_before_submitted then 1 else 0 end) as is_cart,
max(case when address_orders>=1 and last_touchpoint_before_submitted then 1 else 0 end) as is_address,
max(case when payment_orders>=1 and last_touchpoint_before_submitted then 1 else 0 end) as is_payment,
max(case when submitted_date is not null and last_touchpoint_before_submitted then 1 else 0 end) as is_submitted,
max(case when submitted_date is not null and first_touchpoint then 1 else 0 end) as is_submitted_first_touchpoint,
max(case when submitted_date is not null then 1 else 0 end) as is_submitted_any_touchpoint,
max(case when paid_date is not null and last_touchpoint_before_submitted then 1 else 0 end) as is_paid,
max(case when om.new_recurring='RECURRING' and last_touchpoint_before_submitted then 1 else 0 end) as is_recurring_customer,
listagg(voucher_code, ' | ' ) as vouchers_used
from web.sessions s 
left join web.session_order_mapping om 
 on om.session_id=s.session_id 
 group by 1;

GRANT SELECT ON web.session_conversions TO tableau;
GRANT SELECT ON web.session_conversions TO redash_growth;
