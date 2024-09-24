drop table if exists ods_production.order_conversion_labels;
create table ods_production.order_conversion_labels as 
select 
	order_id,
	case when is_in_salesforce=1 then 1 when step is not null then 1 else 0 end as cart_page_orders,
	case when is_in_salesforce=1 then 1 when (step is not null and is_user_logged_in=1) then 1 else 0 end as cart_logged_in_orders,
	case when is_in_salesforce=1 then 1 when (coalesce(step,'cart') in ('cart')) then 0 else 1 end as address_orders,
	case when is_in_salesforce=1 then 1 when (coalesce(step,'cart') in ('cart','address')) then 0 else 1 end as home_address_orders,
	case when is_in_salesforce=1 then 1 when coalesce(step,'cart')  in ('cart','address','home_address','homeAddress') then 0 else 1 end as payment_orders,
	case when is_in_salesforce=1 then 1 when coalesce(step,'cart')  in ('cart','address','home_address','homeAddress','payment') then 0 else 1 end as review_orders,
	case when is_in_salesforce=1 then 1 when coalesce(step,'cart')  in ('cart','address','home_address','homeAddress','payment','review') then 0 else 1 end as summary_orders,
	case when (is_in_salesforce=1 OR (submitted_date is not null and order_mode = 'FLEX')) THEN 1 ELSE 0 END AS completed_orders,
    case when status = 'DECLINED' THEN 1 ELSE 0 END AS declined_orders,
    case when status = 'CANCELLED' THEN 1 ELSE 0 END AS cancelled_orders,
    case when status = 'PAID' THEN 1 ELSE 0 END AS paid_orders,
    case
      when status in ('FAILED FIRST PAYMENT','FAILED_FIRST_PAYMENT') THEN 1
      ELSE 0
    END AS failed_first_payment_orders
from ods_production."order";

GRANT SELECT ON ods_production.order_conversion_labels TO tableau;
