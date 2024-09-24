drop table if exists ods_data_sensitive.credit_bureau_spain;
create table ods_data_sensitive.credit_bureau_spain as
Select 
	s.subscription_id
	,case when s.dpd > 30 and s.dpd <=60  then '1'
		when s.dpd > 60 and s.dpd <=90  then '2'
		when s.dpd > 90 and s.dpd <=120  then '3'
		when s.dpd > 120 and s.dpd <=150  then '4'
		when s.dpd > 150 and s.dpd <=180  then '5'
		when s.dpd > 180  then '6'	
		end as payment_situation
	,s.start_date
	,s.minimum_cancellation_date as end_date
	,s.next_due_date as first_unpaid_due_date
	,last_billing_period_start as last_unpaid_due_date
	,failed_subscriptions as unpaid_instalments_count
	,s.outstanding_subscription_revenue  as unpaid_instalments_amount
	,first_name as customer_firstname
	,last_name as customer_lastname
	,birthdate as customer_birthdate
	,identification_number as customer_document_number
	,case when identification_type = 'national_id' then '01' 
		else null end as customer_document_type
	,'724' as customer_document_country
	,phone_number as customer_phone
	,street as billing_address1
	,house_number as billing_address2
	,cpro::varchar+cmun::varchar+dc::varchar as billing_ine_municipal_code
	,billing_city
	,cpro as billing_ine_province_code
	,billing_zip as billing_zipcode
	,current_timestamp as update_date
	 from master.subscription s 
	left join ods_production.subscription_cashflow cf on s.subscription_id = cf.subscription_id
	left join ods_data_sensitive.customer_pii cp on s.customer_id = cp.customer_id
	left join stg_api_production.personal_identifications pii on pii.user_id = s.customer_id
	left join stg_external_apis.experian_municipal_code_mapping mp on lower(mp."nombre ") = lower(cp.billing_city)
where country_name = 'Spain'
and cp.customer_type = 'normal_customer'
and s.dpd > 30 --to consider only subs with 30+ dpd
and s.first_asset_delivery_date is not null --To Eliminate the non-delivered subscriptions
and unpaid_instalments_count > 0; --To eliminate the non-failed subscriptions