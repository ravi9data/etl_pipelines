DROP TABLE IF EXISTS ods_data_sensitive.payment_info;
CREATE TABLE ods_data_sensitive.payment_info as 
with payment as (
	SELECT DISTINCT * FROM 
	(SELECT 
			spc.id::text as payment_id,
			spc.status__c::TEXT as status,
			spc.type__c::text as type_, 
			spc.payment_method__c::text as payment_method,
			spc.date_due__c::timestamp as due_date,
			spc.date_failed__c::timestamp as failed_date,
			spc.date_paid__c::timestamp as paid_date,
			spc.message__c::text as reason,
			spc.tries_charge__c:: int as tries,
			spc.charge_id__c as charge_id,
			amount_f_due__c as amount_due,
            spc.order__c as order_no
		FROM stg_salesforce.subscription_payment__c spc
		UNION ALL
		  	SELECT 
			rpc.id::text as payment_id,
			rpc.status__c::TEXT as status,
			rpc.type__c::text as type_, 
			rpc.payment_method__c::text as payment_method,
			rpc.date_paid__c::timestamp as due_date,
			rpc.date_failed__c::timestamp as failed_date,
			rpc.date_paid__c::timestamp as paid_date,
			rpc.message__c::text as reason,
			rpc.tries__c::int as tries,
			rpc.charge_id__c as charge_id,
			rpc.amount_refunded__c as amount_due,
            rpc.order__c as order_no
		FROM stg_salesforce.refund_payment__c rpc
			UNION ALL
		  	SELECT 
			apc.id::text as payment_id,
			apc.status__c::TEXT as status,
			apc.type__c::text as type_, 
			apc.payment_method__c::text as payment_method,
			coalesce(booking_date__c,apc.date_Paid__c::timestamp) as due_date,
			apc.date_failed__c::timestamp as failed_date,
			apc.date_paid__c::timestamp as paid_date,
			apc.message__c::text as reason,
			apc.tries_charge__c::int as tries,
			apc.charge_id__c as charge_id,
			apc.amount_paid__c as amount_due,
            apc.order__c as order_no
		FROM stg_salesforce.asset_payment__c apc)
	where due_date::Date < current_date),
failed_group as 
	(select 
		payment.payment_id,
		payment.reason as failed_reason,
		fg.reasons as failed_reason_grouped
	from payment 
	left join stg_external_apis.gs_1_failed_reasons as fr 
		on fr.failreasons = payment.reason
	left join stg_external_apis.gs_2_failed_reasons as fg
		on fr.group = fg.group)
select 
	payment.*,
	o.spree_order_number__c as order_id,
    failed_group.failed_reason_grouped,
    o.totalamount as order_value,
    u.id as customer_id,
    u.user_type as customer_type,
    u.email
from payment
	left join failed_group 
	    on payment.payment_id = failed_group.payment_id
    left join stg_salesforce.order o 
        on payment.order_no = o.id 
    left join stg_api_production.spree_users u
        on o.spree_customer_id__c = u.id
