drop table if exists dm_recommerce.asset_purchase_option;
create table dm_recommerce.asset_purchase_option as
with b as (
select
	distinct 
	subscription_id,
	sum(amount_refunded) as amount_refunded
from
	master.refund_payment rp
where
 reason = 'Transferred Manually'
group by
	1 ),
recuring_voucher_subscripttions as
(
	SELECT 
	s.subscription_id ,
	o.voucher_discount
	FROM 
	master.subscription s 
		left join
		ods_production.order o
		on o.order_id = s.order_id 
	where 
		o.is_voucher_recurring is true

)
select
	a.serial_number ::varchar,
	a.asset_id ,
	a.asset_status_original ,
	a.asset_condition_spv as asset_condition_valuation,
	s.start_date::date,
	s.subscription_sf_id,
	s.status  as subscription_status,
	s.category_name,
	s.product_name,
	s.subcategory_name,
	s.brand,
	s.months_required_to_own,
	s.subscription_value,
	s.subscription_duration ,
	s.net_subscription_revenue_paid,
	COALESCE(b.amount_refunded,0) as amount_refunded_manually_transfered,
	case 
		when vs.subscription_id is not null then (months_required_to_own::float * (s.subscription_value+sp.amount_voucher))
		else (months_required_to_own::float * s.subscription_value) 
	end as min_eligibility_amount,
	(net_subscription_revenue_paid + amount_refunded_manually_transfered) as paid_amount,
	case when paid_amount>=min_eligibility_amount then 0 else min_eligibility_amount-paid_amount end as purchase_option,
	a.residual_value_market_price ,
	a.last_month_residual_value_market_price ,
	a.asset_value_linear_depr 
from
	master.asset a 
	left join
	master.subscription s
	ON a.subscription_id = s.subscription_id 
	left join b 
	on s.subscription_id = b.subscription_id
		left join master.customer c 
		on c.customer_id = s.customer_id
			left join 
			recuring_voucher_subscripttions vs
			on s.subscription_id = vs.subscription_id 
				left join 
				master.subscription_payment sp 
				on sp.subscription_id = s.subscription_id 
				and sp.payment_number = 1
			
;

