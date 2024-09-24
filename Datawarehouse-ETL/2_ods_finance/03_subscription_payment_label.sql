drop table if exists ods_production.subscription_payment_label;
create table ods_production.subscription_payment_label as 
 with pending as (
	select subscription_id,count(*) as pending_subs
	from ods_production.payment_subscription 
	where status = 'PENDING'
	group by 1
	),
	avg_amt as
	(select 
		subscription_id, 
		avg(amount_subscription+amount_voucher)::decimal(10,2) as net_sub_amount
		from
	  ods_production.payment_subscription  
	  group by 1),
	 a as (
	select 
	s.subscription_id, 
	s.subscription_name,
	s.rental_period,
	s.customer_id,
	s.order_id,
	s.subscription_value,
	aa.net_sub_amount,
	s.start_date,
	sc.default_date::date as next_due_date,
	s.subscription_duration,
	s.first_asset_delivery_date,
	sa.first_asset_delivered,
	s.status,
	s.payment_method,
	s.minimum_cancellation_date,
	sc.max_cashflow_date,
	sa.days_on_rent,
	sa.months_on_rent,
	CEIL((days_on_rent::decimal(10,2)/30)) as duration_in_months,
	CEIL((days_on_rent - FLOOR(duration_in_months*0.5))::decimal(10,2)/30) as duration_adjusted,
	sc.payment_count,
	sc.max_payment_number_with_refunds,
	sc.paid_subscriptions,
	sc.refunded_subscriptions,
	sc.failed_subscriptions,
	sc.planned_subscriptions,
	sc.chargeback_subscriptions,
	sc.last_payment_status,
	sc.last_valid_payment_category,
	sc.last_billing_period_start,
	sc.dpd,
	sc.outstanding_subscription_revenue,
	sa.allocated_assets,
	sa.replacement_assets,
	sa.delivered_assets,
	sa.returned_packages,
	sa.outstanding_assets,
	sa.outstanding_purchase_price,
	sa.damaged_allocations
	from ods_production.subscription s 
	left join ods_production.subscription_cancellation_reason cr on s.subscription_id = cr.subscription_id
	left join ods_production.subscription_assets sa on sa.subscription_id = s.subscription_id
	left join ods_production.subscription_cashflow sc on sc.subscription_id = s.subscription_id
	left join avg_amt aa on aa.subscription_id = s.subscription_id
	where status = 'ACTIVE'
	--and start_date > '2019-01-01'
	),
	labels as 
	(select 	a.*,coalesce(pending_subs,0) as pending_subscriptions,	
	case when allocated_assets is null and (refunded_subscriptions > 0 or chargeback_subscriptions > 0)
				then 'Active, Not Allocated, Refunded' 
		 when allocated_assets is null and refunded_subscriptions = 0 and chargeback_subscriptions = 0
				then 'Active, Not Allocated, No Refund' 	
		when allocated_assets >= 1 and delivered_assets = 0 and (refunded_subscriptions > 0 or chargeback_subscriptions > 0)
		       then 'Active, Not Delivered, Refunded'
		when allocated_assets >= 1 and delivered_assets = 0 and refunded_subscriptions = 0 and chargeback_subscriptions = 0
		       then 'Active, Not Delivered, No Refund'   
		 when  delivered_assets = 1 and first_asset_delivery_date is null and paid_subscriptions < 2 and outstanding_assets = 0
		 		then 'Delivered, No Payment Cycle, Returned'
		 when  delivered_assets = 1 and first_asset_delivery_date is null and paid_subscriptions < 2 and outstanding_assets > 0
		 		then 'Delivered, No Payment Cycle'
		when replacement_assets > 0 and outstanding_assets > 1
			    then 'Replacement Subscription - 1+ Outstanding'
		 when replacement_assets > 0 and outstanding_assets > 0
			    then 'Replacement Subscription - 1 Outstanding'
		when replacement_assets > 0 and outstanding_assets = 0
			    then 'Replacement Subscription - No Outstanding'		    
		 when allocated_assets > 0 and outstanding_assets = 0 and failed_subscriptions > 0
		 		then 'Active, No Outstanding assets, Failed payment'
		 when allocated_assets > 0 and outstanding_assets = 0 
		 		then 'Active, No Outstanding assets'	
		when last_valid_payment_category = 'NYD' and last_payment_status = 'HELD' 		
		 	  then 'HELD status'  --To be investigated
		 when payment_count > 1 and  last_billing_period_start < CURRENT_DATE-30 and failed_subscriptions = 0
		       then 'Payment Cycle interruption'
		when ((last_valid_payment_category like '%DEFAULT%' and last_valid_payment_category not like '%RECOVERY%') or last_valid_payment_category like '%DELINQUENT%'  or last_valid_payment_category like '%ARREARS%')
		  and failed_subscriptions > 3 then 'Active, +3 Failed Payments'	       
		 when ((last_valid_payment_category like '%DEFAULT%' and last_valid_payment_category not like '%RECOVERY%') or last_valid_payment_category like '%DELINQUENT%'  or last_valid_payment_category like '%ARREARS%')
		  and failed_subscriptions > 0 then 'Active,1-3 Failed Payments'
		  when ((last_valid_payment_category like '%DEFAULT%' and last_valid_payment_category not like '%RECOVERY%') or last_valid_payment_category like '%DELINQUENT%')
			and failed_subscriptions = 0   then 'Issues with payment subs' --refer Sf link
			when last_valid_payment_category = 'NYD' and failed_subscriptions > 0
			then 'NYD -Failed Today'
			when (payment_count = paid_subscriptions) and last_valid_payment_category = 'NYD' and pending_subscriptions > 0
			then 'All Paid - with Pending status'
			when (payment_count = paid_subscriptions + refunded_subscriptions) and (last_billing_period_start>max_cashflow_date)
			then 'Advance payment'
			when (payment_count = paid_subscriptions + refunded_subscriptions) 
			then 'All Paid - No issues'
			when (payment_count = paid_subscriptions + refunded_subscriptions +planned_subscriptions)
			then 'All Paid - No issues - Due Today'	
			else 'Others'
				end as payment_label,
		case when payment_label in ('Delivered, No Payment Cycle, Returned',
									'Delivered, No Payment Cycle',
									'Replacement Subscription - 1+ Outstanding') 
									then 'High'
			 when payment_label in ('Active, No Outstanding assets, Failed payment',
			 						'Active, No Outstanding assets','HELD status',
			 						'Payment Cycle interruption','Issues with payment subs',
			 						'Active, Not Delivered, Refunded') 
			 						then 'Medium'
			else 'Low'	 						
			end as label_criticality,
			case when delivered_assets > 0
			then (months_on_rent  - max_payment_number_with_refunds)  
			else 0 end as difference,
			case  when outstanding_assets > 1 and replacement_assets > 0
				then (outstanding_purchase_price/outstanding_assets) + ((duration_adjusted - max_payment_number_with_refunds)* net_sub_amount)
				when replacement_assets > 0
				then ((duration_adjusted - max_payment_number_with_refunds)* subscription_value)
				when payment_label = 'All Paid - No issues' and (difference = -1)and (max_cashflow_date::date = current_date) 
				then 0
				when payment_label = 'All Paid - No issues' and dpd = -1 
				then 0
				when delivered_assets > 0
			then ((months_on_rent - max_payment_number_with_refunds)* net_sub_amount)
			else 0 end as pending_value
	from a 
	left join pending p on a.subscription_id = p.subscription_id
	) select * from labels;

GRANT SELECT ON ods_production.subscription_payment_label TO tableau;
