DROP TABLE IF EXISTS stg_kafka_subscription; 
	CREATE TEMP TABLE stg_kafka_subscription
	SORTKEY(subscription_id)
	DISTKEY(subscription_id)
    AS
with buyout_terms AS (
	SELECT 
		nullif(JSON_EXTRACT_PATH_text(payload,'id'),'') AS id,
		event_name,
		JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'price_next_period') AS price,		
		TRUE AS buyout_disabled,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'),'discounts'),0,TRUE),'reason') AS buyout_disabled_reason,
		event_timestamp AS buyout_disabled_at
	FROM stg_kafka_events_full.stream_customers_contracts_v2
	WHERE event_name = 'purchase_option_disabled'
)	
, stg_kafka_v2 as (
select
		event_timestamp,
		v.event_name,
		version,
		payload,
		nullif(JSON_EXTRACT_PATH_text(payload,'id'),'')as id,
		nullif(JSON_EXTRACT_PATH_text(payload,'type'),'')as type,
		nullif(JSON_EXTRACT_PATH_text(payload,'user_id'),'')as user_id,
		nullif(JSON_EXTRACT_PATH_text(payload,'billing_account_id'),'')as billing_account_id,
		nullif(JSON_EXTRACT_PATH_text(payload,'order_number'),'')as order_number,
		nullif(JSON_EXTRACT_PATH_text(payload,'state'),'')as state,
		nullif(JSON_EXTRACT_PATH_text(payload,'created_at'),'')as created_at,
		nullif(JSON_EXTRACT_PATH_text(payload,'activated_at'),'')as activated_at,
		nullif(JSON_EXTRACT_PATH_text(payload,'terminated_at'),'')as terminated_at,
		nullif(JSON_EXTRACT_PATH_text(payload,'termination_reason'),'')as termination_reason,
		nullif(JSON_EXTRACT_PATH_text(payload,'goods'),'')as goods,
		nullif(JSON_EXTRACT_PATH_text(payload,'billing_terms'),'')as billing_terms,
		nullif(case when v.event_name = 'extended' 
            THEN json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'purchase_term'), 'new'), 'months_required')
            ELSE JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'purchase_term'), 'months_required') END, '') as months_required,
		COALESCE(b.price,nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'price')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'price') end, '')) as price,
		nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'currency')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'currency') end,'') as currency,
		nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'type')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'type') end,'') as billing_type,
		nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'current_period')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'current_period') end,'') as current_period,
		nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'new'), 'next_period')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_terms'), 'next_period') end,'') as next_period,
		nullif(case when v.event_name = 'extended' 
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'duration_terms'),'new'), 'committed_length')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'duration_terms'), 'committed_length') end,'') as committed_length,	
		nullif(JSON_EXTRACT_PATH_text((json_extract_array_element_text(goods,0)),'variant_sku'),'')as variant_sku,
		nullif(json_extract_path_text(json_extract_path_text(payload, 'ending_terms'),'available_after'),'')as available_after,
		COALESCE (b.buyout_disabled,FALSE) AS buyout_disabled,
		b.buyout_disabled_at,
		b.buyout_disabled_reason,
		count(*) over (partition by nullif(JSON_EXTRACT_PATH_text(v.payload,'id'),'')) as total_events,
		row_number () over (partition by nullif(JSON_EXTRACT_PATH_text(v.payload,'id'),'') order by v.event_timestamp DESC,v.event_name DESC) as idx --inserted event_name as event purchase_option_disabled has the same timestamp of discount_applied 
	from stg_kafka_events_full.stream_customers_contracts_v2 v
	LEFT JOIN buyout_terms b
		ON b.id = nullif(JSON_EXTRACT_PATH_text(v.payload,'id'),'')
		AND v.event_name = b.event_name
	)
, initial_terms AS -- Note we can ALSO use this subquery TO obtain original price IF neccessary.
	(
	SELECT
		id,
		price::double precision * COALESCE(fx.exchange_rate_eur, 1) as initial_price, --calculate EUR CSV
		committed_length AS initial_rental_term,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_timestamp ASC) row_num -- GET FIRST creation event IN CASE OF duplicates.
	FROM stg_kafka_v2 s
	LEFT JOIN trans_dev.daily_exchange_rate fx
	  ON s.event_timestamp::date = fx.date_
	  AND UPPER(s.currency) = fx.currency
	  AND fx.currency <> 'EUR'
	WHERE event_name = 'created'
	)	
	,sub_dates as
	(
	select
      id as subscription_id,
      max(case when state = 'fulfilling' then event_timestamp::timestamp end) as start_date,
      min(case when state = 'active' then event_timestamp::timestamp end) as active_date,
      max(case when state = 'active' then event_timestamp::timestamp end) as max_active_date,
      max(case when state in ('cancelled','ended','refused') then event_timestamp::timestamp end) as cancellation_date,
      max(case when state in ('cancelled','ended','refused') AND termination_reason ='handed_over_to_dca' then event_timestamp::timestamp end) AS debt_collection_handover_date,
      max(case when state = 'paused' then event_timestamp::timestamp end) as paused_date,
      max(case when event_name in('reactivated','revived') then event_timestamp::timestamp end) as reactivated_date
	from stg_kafka_v2
  --where type ='flex'
	group by 1
	)
    ,idx as (
	select skv.*,
	ROW_NUMBER() over (partition by order_number,variant_sku order by created_at asc) as c_idx
	from stg_kafka_v2 skv
		left join ods_production.contracts_deleted_in_source cds on cds.contract_id = skv.id
	where
		idx = 1
		and cds.id is null /*exclude contracts deleted in source system */
	)
	,last_event_pre as
	(
	select
		le.*,is_pay_by_invoice, add_months(activated_at::timestamp , committed_length::int) min_cancellation_date
	from idx le
	left join ods_production."order" o
	on le.order_number = o.order_id
	where (o.paid_date is not null or
	((is_pay_by_invoice is true and state = 'active') or (is_pay_by_invoice is true and state in('ended', 'cancelled'))  or (is_pay_by_invoice is true and state = 'paused')))
	)
	,res
	as (
	SELECT
	order_id ,sku_variant_code,fulfilled_at 
	FROM
		ods_production.inventory_reservation ir
	WHERE
		order_mode = 'flex'
		AND ((initial_quantity::int > quantity::int
			AND status = 'paid')
		OR status = 'fulfilled')
	)
	,last_event as (
	select l.*,
	case when r.order_id is not null then true else false end is_fulfilled
	from last_event_pre l
	left join res r on l.order_number = r.order_id and l.variant_sku = r.sku_variant_code)
	,order_tax_pre_ as
	(
	select
			json_extract_path_text(payload,'country_code') as country_code,
			JSON_EXTRACT_PATH_text(payload,'orders') as order_number_array,
			json_extract_array_element_text(order_number_array,0) as order_payload,
			json_extract_path_text(order_payload,'number') as order_id,
			json_extract_path_text(order_payload,'tax_rate') as tax_rate,
			rank() over (partition by order_id order by kafka_received_at desc) as idx,
			kafka_received_at as payment_event_timestamp
		from
		stg_curated.stg_internal_billing_payments
			where  is_valid_json(payload)
	),
	order_tax as
	(
	select * from order_tax_pre_ where idx = 1
	)
	select
	distinct
	s.id as subscription_id,
	null as subscription_name,
	null as subscription_bo_id,
	null as order_item_sf_id,
	null::DOUBLE PRECISION as order_item_id,
	COALESCE(it.initial_rental_term::int, committed_length::int)::DOUBLE PRECISION as rental_period,
	cast(coalesce(it.initial_rental_term::int,committed_length::int,0) as varchar)||' Months'  as subscription_plan,
	store_id,
	s.order_number as order_id,
	o.customer_id,
	NULL::TIMESTAMP as customer_acquisition_date_temp,
	--NULL::BIGINT as subscriptions_per_customer, Replaced in union query
	--NULL::BIGINT as rank_subscriptions,Replaced in union query
	--case
	--	when ot.country_code= 'us' then round((s.price::decimal(10,2) + (ot.tax_rate::decimal(10,2) * s.price::decimal(10,2)))::DOUBLE PRECISION,2)
	--	else s.price::double precision
	--end as subscription_value,
	s.price::double precision as subscription_value,
	--case
	--	when ot.country_code= 'us' then round((s.price::decimal(10,2) + (ot.tax_rate::decimal(10,2) * s.price::decimal(10,2)))::DOUBLE PRECISION,2) * coalesce(committed_length::int,0)
	--	else s.price::double precision * coalesce(committed_length::int,0)
	--end as committed_sub_value,
	coalesce(it.initial_price::double precision, s.price::double precision) * coalesce(it.initial_rental_term::int,committed_length::int,0)  AS committed_sub_value,
	upper(s.currency) as currency,
	/*(case
 	when upper(s.currency) !='EUR' and exc.date_ is not null then (round((s.price::decimal(10,2) + (ot.tax_rate::decimal(10,2) * s.price::decimal(10,2)))::DOUBLE PRECISION,2)  * exc.exchange_rate_eur)
 	when upper(s.currency) !='EUR' and exc.date_ is null then (round((s.price::decimal(10,2) + (ot.tax_rate::decimal(10,2) * s.price::decimal(10,2)))::DOUBLE PRECISION,2)  * exc_last.exchange_rate_eur)
	when upper(s.currency) ='EUR' then price::double precision
 	else price::double precision
	end)::decimal(10,2) as subscription_value_euro,
	*/
	(case
 	when upper(s.currency) !='EUR' and exc.date_ is not null then (price::double precision * exc.exchange_rate_eur)
 	when upper(s.currency) !='EUR' and exc.date_ is null then (price::double precision * exc_last.exchange_rate_eur)
	when upper(s.currency) ='EUR' then price::double precision
 	else price::double precision
	end)::decimal(10,2) as subscription_value_euro,
	start_date,
	active_date as first_asset_delivery_date,
	case when first_asset_delivery_date is not null then true else false end as asset_was_delivered,
	case
		when active_date is null and max_active_date is null and cancellation_date is not null then 'CANCELLED'
		when cancellation_date >= active_date and max_active_date is null then 'CANCELLED'
		when cancellation_date >= max_active_date then 'CANCELLED'
		when cancellation_date is null then 'ACTIVE'
		when active_date is null and max_active_date is null and cancellation_date is null then 'ACTIVE'
		else 'ACTIVE' end  as status,
	COALESCE(
        CASE
            WHEN status = 'CANCELLED'::text THEN COALESCE(cancellation_date,start_date)
            ELSE NULL::timestamp without time zone
        END,
        CASE
            WHEN status::text = 'ACTIVE'::text THEN 'now'::text::date + 0
            ELSE NULL::date
        END::timestamp without time zone)::date - COALESCE(first_asset_delivery_date, start_date)::date AS  subscription_duration,
      (months_between( COALESCE(
        CASE
            WHEN status::text = 'CANCELLED'::text THEN COALESCE(cancellation_date,start_date)
            ELSE NULL::timestamp without time zone
        END,
        CASE
            WHEN status::text = 'ACTIVE'::text THEN 'now'::text::date + 0
            ELSE NULL::date
        END::timestamp without time zone)::date,COALESCE(first_asset_delivery_date, start_date)::date ))::decimal(10,2) as subscription_duration_in_months ,
	NULL::TIMESTAMP  as renewal_date,
	NULL::TIMESTAMP  as next_renewal_date,
	NULL::TIMESTAMP  as cancellation_requested_date,
	case 
		when cancellation_date >= active_date and max_active_date is null then cancellation_date
		when cancellation_date >= active_date and cancellation_date >= max_active_date then cancellation_date
		when cancellation_date >= max_active_date then cancellation_date
		when active_date is null and cancellation_date is not null then cancellation_date
		when max_active_date is not null and cancellation_date is not null and max_active_date > cancellation_date then null
		else null
		END::timestamp without time zone as cancellation_date,	
	NULL as cancellation_note,
	replace(termination_reason, '_', ' ') as cancellation_reason,
	NULL as cancellation_reason_dropdown,
	payment_method,
	start_date as created_date,
	s.event_timestamp::TIMESTAMP  as updated_date,
	case
		when is_fulfilled is true then 'ALLOCATED'
		when aa.allocation_status_original is not null then 'ALLOCATED'
		when (active_date is not null and start_date < '2021-10-01') then 'ALLOCATED'
		else 'PENDING ALLOCATION'
	end as allocation_status,
	NULL::INT as cross_sale_attempts,
	NULL::INT as manual_allocation_attempts,
	NULL::DOUBLE PRECISION as allocation_tries,
	nvl(s.available_after::TIMESTAMP, min_cancellation_date) as minimum_cancellation_date,
	datediff('d',start_date,cancellation_date) as days_to_cancel,
	committed_length::DOUBLE PRECISION as minimum_term_months,
	product_name as order_product_name, --from ods product
	NULL::TIMESTAMP as payments_last_run_date,
	NULL::TIMESTAMP  as coeo_claim_date__c,
	NULL as coeo_claim_id__c,
	NULL as agency_for_dc_processing__c,
	NULL as dc_agency_case_id__c,
	NULL as dc_status,
	sd.debt_collection_handover_date::TIMESTAMP  as debt_collection_handover_date,
	NULL::CHARACTER VARYING as result_debt_collection_contact,
	sd.reactivated_date,
	NULL::INT as replacement_attempts,
	v.variant_sku,
	False as is_bundle,
	country_name,
	o.store_commercial,
	st.store_name,
	st.store_label,
	st.store_short,
	st.account_name,
	case when p.product_sku is not null then p.product_sku else split_part(s.variant_sku,'V',1) end as product_sku,
	p.product_name,
	p.category_name,
	p.subcategory_name,
	p.brand,
	NULL::INT as trial_days,
	FALSE as trial_variant,
	coalesce(h.months_required::varchar, s.months_required) as months_required_to_own,
	s.buyout_disabled,
	s.buyout_disabled_reason,
	s.buyout_disabled_at,
	s.state
	from
	last_event s
	left join sub_dates sd
	on s.id = sd.subscription_id
	left join trans_dev.daily_exchange_rate exc
   		 ON (CASE WHEN cancellation_date::date <= '2022-07-31' THEN sd.start_date::date 
    			 WHEN cancellation_date::date IS NOT NULL THEN (cast (date_trunc('month', cancellation_date) - interval '1 day' as date))
    			 ELSE (cast (date_trunc('month', current_date) - interval '1 day' as date)) END ) 
    		 		--> If the subscription was cancelled before August, it will consider the fx rate of the start_date of the subscription.
    		 			-- If subscription was cancelled after August, it will consider the fx rate of the last day of the month before the cancellation date
    		 			-- otherwise, it will consider the fx rate of the last day of the previous month
    			= exc.date_
	AND upper(s.currency)  = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
    ON upper(s.currency) = exc_last.currency
    left join stg_kafka_events_full.allocation_us aa
    on s.id = aa.subscription_id
	left join ods_production."order" o
	on s.order_number = o.order_id
	left join ods_production.store st
	on st.id = o.store_id
	left join ods_production.variant v
	on s.variant_sku = v.variant_sku
	left join ods_production.product p
	on p.product_id = v.product_id or (v.product_id is null and split_part(s.variant_sku,'V',1) = p.product_sku)
	left join order_tax ot
	on ot.order_id = s.order_number
	left join ods_production.new_infra_missing_history_months_required h --> this is a static table and won't generate locks. 
	on s.id =  h.contract_id 
	LEFT JOIN initial_terms it
	ON s.id = it.id	
	AND it.row_num = 1;



DROP TABLE IF EXISTS subs_new_infra;
	CREATE TEMP TABLE subs_new_infra
	SORTKEY(subscription_id)
	DISTKEY(subscription_id)
as
	select 
	s.subscription_id,
	s.subscription_name,
	s.subscription_bo_id,
	s.order_item_sf_id,
	s.order_item_id,
	s.rental_period,
	s.subscription_plan,
	s.store_id,
	s.order_id,
	s.customer_id,
	s.customer_acquisition_date_temp,
	s.subscription_value,
	s.committed_sub_value,
	s.currency,
	s.subscription_value_euro,
	s.start_date,
	s.first_asset_delivery_date,
	s.asset_was_delivered,
	s.status,
	s.subscription_duration,
	s.subscription_duration_in_months ,
	s.renewal_date,
	s.next_renewal_date,
	s.cancellation_requested_date,
	s.cancellation_date,  
	s.cancellation_note,
	s.cancellation_reason,
	s.cancellation_reason_dropdown,
	s.payment_method,
	s.created_date,
	s.updated_date,
	s.allocation_status,
	s.cross_sale_attempts,
	s.manual_allocation_attempts,
	s.allocation_tries,
	s.minimum_cancellation_date,
	s.days_to_cancel,
	s.minimum_term_months,
	s.order_product_name,
	s.payments_last_run_date,
	s.coeo_claim_date__c,
	s.coeo_claim_id__c,
	s.agency_for_dc_processing__c,
	s.dc_agency_case_id__c,
	s.dc_status,
	s.debt_collection_handover_date,
	s.result_debt_collection_contact,
	s.reactivated_date,
	s.replacement_attempts,
	s.variant_sku,
	s.is_bundle,
	s.country_name,
	s.store_commercial,
	s.store_name,
	s.store_label,
	s.store_short,
	s.account_name,
	s.product_sku,
	s.product_name,
	s.category_name,
	s.subcategory_name,
	s.brand,
	s.trial_days,
	s.trial_variant,
	s.months_required_to_own,
	(case
 	when upper(s.currency) !='EUR' and exc.date_ is not null then (subscription_value::double precision * exc.exchange_rate_eur)
 	when upper(s.currency) !='EUR' and exc.date_ is null then (subscription_value::double precision * exc_last.exchange_rate_eur)
	when upper(s.currency) ='EUR' then subscription_value::double precision
 	else subscription_value::double precision
	end)::decimal(10,2) as reporting_subscription_value_euro,
  	s.buyout_disabled as buyout_disabled,
	s.buyout_disabled_at::timestamp as buyout_disabled_at,
	CASE WHEN s.buyout_disabled_reason = 'existing_contract_discount' THEN 'existing_discount' ELSE s.buyout_disabled_reason END as buyout_disabled_reason,
	s.state
	from stg_kafka_subscription s
	left join  trans_dev.daily_exchange_rate exc  
	   ON   upper(s.currency)  = exc.currency
   and 
   ( 
   	(cast (date_trunc('month', current_date) - interval '1 day' as date) = exc.date_)
    )
    LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
    ON upper(s.currency) = exc_last.currency;


   
DROP TABLE IF EXISTS ods_subscription_legacy;
	CREATE TEMP TABLE ods_subscription_legacy
	SORTKEY(subscription_id)
	DISTKEY(subscription_id)
as
with bundle as (
select distinct subscription_id
from ods_production.allocation
where is_bundle)
,trial_products as (
  select distinct
variant_id
from stg_api_production.spree_line_items
where trial_days is not null)
SELECT distinct
    s.id as subscription_id,
    s.name as subscription_name,
    s.subscription_id__c as subscription_bo_id,
    s.order_product__c as order_item_sf_id,
    i.spree_order_line_id__c as order_item_id,
--    s.number__c as subscription_number,
    case when coalesce(i.minimum_term_months__c,0)::int = 0 then 1 else coalesce(i.minimum_term_months__c,1) end as rental_period,
    case when coalesce(i.minimum_term_months__c,0)::int = 0 then 'Pay As You Go' else cast(coalesce(i.minimum_term_months__c,0) as varchar)||' Months' end as subscription_plan,
    cast(o.store_id__c as VARCHAR) as store_id,
    o.spree_order_number__c as order_id,
    cast(c.spree_customer_id__c as integer) as customer_id,
    min(COALESCE(s.date_start__c, NULL::timestamp )) OVER (PARTITION BY c.spree_customer_id__c::INT) AS customer_acquisition_date_temp,
   -- count(*) OVER (PARTITION BY c.spree_customer_id__c::INT) AS subscriptions_per_customer, (Replaced in union query )
    --rank() OVER (PARTITION BY c.spree_customer_id__c::INT ORDER BY s.date_start__c, s.createddate, s.amount__c) AS rank_subscriptions, (Replaced in union query )
    s.amount__c as subscription_value,
    COALESCE(i.unitprice, s.amount__c) * COALESCE(rental_period, s.minimum_term_months__c) AS committed_sub_value,
    s.currency__c as currency,
    s.amount__c::decimal(10,2) as subscription_value_euro,
    s.date_start__c as start_date,
    s.date_first_asset_delivery__c as first_asset_delivery_date,
    CASE WHEN s.date_first_asset_delivery__c IS NOT NULL THEN true ELSE false END AS asset_was_delivered,
    case when s.date_cancellation__c is not null then 'CANCELLED' else 'ACTIVE' end as status,
        COALESCE(
        CASE
            WHEN s.status__c::text = 'CANCELLED'::text THEN COALESCE(s.date_cancellation_requested__c, s.date_cancellation__c, s.date_first_asset_delivery__c, s.date_start__c)
            ELSE NULL::timestamp without time zone
        END,
        CASE
            WHEN s.status__c::text = 'ACTIVE'::text THEN 'now'::text::date + 0
            ELSE NULL::date
        END::timestamp without time zone)::date - COALESCE(s.date_first_asset_delivery__c, s.date_start__c)::date AS  subscription_duration,
      (months_between( COALESCE(
        CASE
            WHEN s.status__c::text = 'CANCELLED'::text THEN COALESCE(s.date_cancellation_requested__c, s.date_cancellation__c, s.date_first_asset_delivery__c, s.date_start__c)
            ELSE NULL::timestamp without time zone
        END,
        CASE
            WHEN s.status__c::text = 'ACTIVE'::text THEN 'now'::text::date + 0
            ELSE NULL::date
        END::timestamp without time zone)::date,COALESCE(s.date_first_asset_delivery__c, s.date_start__c)::date ))::decimal(10,2) as subscription_duration_in_months ,
    s.date_renewal__c as renewal_date,
    s.date_renewal_next__c as next_renewal_date,
    s.date_cancellation_requested__c as cancellation_requested_date,
    s.date_cancellation__c AS cancellation_date,
    s.cancellation_note__c as cancellation_note,
    LOWER (s.cancellation_reason__c) as cancellation_reason,
    lower(cancellation_reason_picklist__c) as cancellation_reason_dropdown,
    s.payment_method__c as payment_method,
    s.createddate as created_date,
    greatest(
         s.lastmodifieddate,
         s.systemmodstamp,
         i.lastmodifieddate,
         i.systemmodstamp,
         ii.updated_at,
         c.lastmodifieddate,
         c.systemmodstamp,
         u.updated_at,
         o.lastmodifieddate,
         o.systemmodstamp,
         store.updated_date,
         variant.variant_updated_at,
         prod.updated_at) as updated_date, --systemmodstamp is always greater than lastmodifieddate
    s.allocation_status__c as allocation_status, --why allocation status is null
    cast(s.cross_sale_attempts__c as integer) as cross_sale_attempts,
    cast(s.manual_allocation_attempts__c as integer) as manual_allocation_attempts,
    s.allocation_tries__c as allocation_tries,
    s.minimum_cancellation_date__c as minimum_cancellation_date,
    datediff('d',s.date_start__c::timestamp,s.date_cancellation__c::timestamp) as days_to_cancel,
    s.minimum_term_months__c as minimum_term_months,
    s.order_product_name__c as order_product_name,
    s.payments_run_last__c as payments_last_run_date,
    s.coeo_claim_date__c,
    s.coeo_claim_id__c,
    s.agency_for_dc_processing__c,
    s.dc_agency_case_id__c,
    CASE
            WHEN s.coeo_claim_date__c IS NOT NULL AND s.coeo_claim_id__c IS NOT NULL AND s.coeo_claim_closed_date__c IS NULL THEN 'DC - COEO'::text
            WHEN s.coeo_claim_date__c IS NOT NULL AND s.coeo_claim_id__c IS NOT NULL AND s.coeo_claim_closed_date__c IS NOT NULL THEN 'DC - COEO CLOSED'::text
            WHEN s.agency_for_dc_processing__c::text = 'PAIR Finance'::text THEN 'DC - PAIR FINANCE'::text
	    when s.agency_for_dc_processing__c::text is not null then s.agency_for_dc_processing__c::text
            WHEN s.coeo_claim_date__c IS NULL AND s.coeo_claim_id__c IS NULL THEN 'NO DC'::text
            ELSE 'NOT CLASSIFIED'::text
        END AS dc_status,
      	CASE
            WHEN s.date_cancellation__c < '2017-07-19 00:00:00'::timestamp
              AND (lower(s.cancellation_reason__c) in ('debt collection - not recoverable', 'in debt collection','in debt collections') )
              or s.agency_for_dc_processing__c::text = 'PAIR Finance'
               THEN coalesce(s.date_cancellation__c,s.coeo_claim_date__c,s.automatic_handover_date__c)
            WHEN s.date_cancellation__c >= '2017-07-19 00:00:00'::timestamp
				THEN	(CASE WHEN  ((agency_for_dc_processing__c is not null) or (dc_agency_case_id__c is not null))
             				THEN coalesce(automatic_handover_date__c,s.date_cancellation__c)
						WHEN ((coeo_claim_date__c is not null) or (coeo_claim_id__c is not null))
							THEN coalesce(s.coeo_claim_date__c,s.date_cancellation__c) END)
            ELSE NULL::timestamp
        END AS debt_collection_handover_date,
       (((COALESCE(c.customer_type__c, ''::character varying)::text ||
        CASE
            WHEN c.customer_type__c IS NULL AND s.dc_customer_contact_result__c IS NULL THEN NULL::text
            ELSE ' '::text
        END) || COALESCE(s.dc_customer_contact_result__c, ''::bpchar::character varying)::text))::character(255) AS result_debt_collection_contact,
		NULL::TIMESTAMP as reactivated_date,
--    s.agency_for_dc_processing__c as agency_for_dc_processing,
--    s.dc_customer_contact_result__c as dc_customer_contact_result,
--    s.dc_deadline__c as dc_deadline_date,
    cast(s.replacement_attempts__c as integer) as replacement_attempts  --
    ,i.f_product_sku_variant__c as variant_sku
    ,case when b.subscription_id is not null then true else false end as is_bundle
	,STORE.country_name
    ,case
     when u.user_type='business_customer' then 'B2B'||' '||case when STORE.country_name='Germany' then STORE.country_name else 'International' end
     when store.store_short in ('Partners Online','Partners Offline') then 'Partnerships'||' '||case when store.country_name='Germany' then STORE.country_name else 'International' end
     else 'Grover'||' '||case when STORE.country_name='Germany' then STORE.country_name else 'International' end
    end as store_commercial
    ,STORE.STORE_NAME
     ,STORE.STORE_LABEL
     ,STORE.STORE_SHORT
     ,STORE.account_name
     ,case when prod.product_sku is not null then prod.product_sku else split_part(i.f_product_sku_variant__c,'V',1) end as product_sku
     ,prod.product_name
	 ,prod.category_name
     ,prod.subcategory_name
     ,prod.brand
     ,ii.trial_days
     ,case when tp.variant_id is not null then true else false end as trial_variant
     ,(CASE
	  WHEN json_extract_path_text(buyout,'months_to_own','months_required') ='' THEN NULL
	   ELSE json_extract_path_text(II.buyout,'months_to_own','months_required') END) AS months_required_TO_OWN,
       s.amount__c::decimal(10,2) as reporting_subscription_value_euro,
      case when JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled')=''
			or JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled')='false'
			or JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled') is null then false  
	  when JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled')='true' then true  end as buyout_disabled,
	  case when	JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled_at')='' then null::timestamp else  JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled_at')::timestamp end as buyout_disabled_at,
	  case when JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled_reason') ='' then NULL::varchar else JSON_EXTRACT_PATH_text(ii.buyout,'buyout_disabled_reason')::varchar end as buyout_disabled_reason,
	  s.status__c as state
 FROM stg_salesforce.subscription__c as s
  left join stg_salesforce.orderitem as i
   on s.order_product__c = i.id
  left join stg_api_production.spree_line_items ii
   on i.spree_order_line_id__c=ii.id
  left join trial_products tp
   on tp.variant_id=ii.variant_id
  left join stg_salesforce.account as c
   on c.id = s.customer__c
  left join stg_api_production.spree_users u
   on u.id=c.spree_customer_id__c
  left join stg_salesforce.order as o
   on s.order__c = o.id
  left join bundle b
   on b.subscription_id=s.id
  LEFT JOIN ODS_PRODUCTION.STORE STORE
   ON STORE.ID=O.store_id__c
  LEFT JOIN ODS_PRODUCTION.VARIANT VARIANT
   ON VARIANT.VARIANT_SKU=i.f_product_sku_variant__c
  LEFT JOIN ODS_PRODUCTION.PRODUCT PROD
   ON PROD.PRODUCT_id=variant.product_id or (variant.product_id is null and split_part(i.f_product_sku_variant__c,'V',1) = PROD.product_sku)
  ;



DROP TABLE IF EXISTS sub_final;
	CREATE TEMP TABLE sub_final
	as
with union_sub as
(
select * from ods_subscription_legacy
union all
select * from subs_new_infra
)
select
*,
min(customer_acquisition_date_temp) over (partition by customer_id) as customer_acquisition_date,
rank() OVER (PARTITION BY customer_id ORDER BY start_date,created_date) as rank_subscriptions,
count(*) OVER (PARTITION BY customer_id::INT) as subscriptions_per_customer
from union_sub;

--Alter table sub_final
--drop column customer_acquisition_date_temp;

drop table if exists sub_final_migrated_dates;
create TEMP table sub_final_migrated_dates as
select s.*,mig.migration_date as migration_date from sub_final s 
LEFT JOIN stg_curated.migrated_contracts mig --staging_airbyte_bi.final_migrated_contracts_dates_3 mig
	on UPPER(s.subscription_bo_id) = UPPER(mig.subscription_bo_id)
	OR UPPER(s.subscription_id) = UPPER(mig.subscription_bo_id);

delete from sub_final_migrated_dates
where subscription_id in (
	select distinct id as subscription_id
	from stg_salesforce.subscription__c sc 
	inner join stg_kafka_subscription sks on sc.subscription_id__c = sks.subscription_id
	--where sc.cancellation_note__c ilike '%infra%' 
);


/*Update missing fields data from old infra data and correct the sub id*/
update sub_final_migrated_dates 
set subscription_id = sc.subscription_id,
	subscription_bo_id = sc.subscription_bo_id,
	subscription_name = sc.subscription_name,
	order_item_sf_id = sc.order_item_sf_id,
	order_item_id = sc.order_item_id,
	rental_period = sc.rental_period,
	subscription_plan = sc.subscription_plan,
	store_id = sc.store_id,
	order_id = sc.order_id,
	customer_id = sc.customer_id,
	subscription_value = coalesce(sub_final_migrated_dates.subscription_value,sc.subscription_value),
	committed_sub_value = coalesce(sub_final_migrated_dates.committed_sub_value,sc.committed_sub_value),
	currency = sc.currency,
	subscription_value_euro =coalesce(sub_final_migrated_dates.subscription_value_euro,sc.subscription_value_euro),
	start_date = sc.start_date,
	first_asset_delivery_date = sc.first_asset_delivery_date,
	asset_was_delivered = sc.asset_was_delivered,
	payment_method = sc.payment_method,
	created_date = sc.created_date,
	allocation_status = coalesce(sc.allocation_status,sub_final_migrated_dates.allocation_status),
	cross_sale_attempts = sc.cross_sale_attempts,
	manual_allocation_attempts = sc.manual_allocation_attempts,
	allocation_tries = sc.allocation_tries,
	minimum_cancellation_date = coalesce(sc.minimum_cancellation_date,sub_final_migrated_dates.minimum_cancellation_date),
	days_to_cancel = coalesce(sc.days_to_cancel,sub_final_migrated_dates.days_to_cancel),
	minimum_term_months = coalesce(sc.minimum_term_months,sub_final_migrated_dates.minimum_term_months),
	order_product_name = sc.order_product_name,
	payments_last_run_date = coalesce(sub_final_migrated_dates.payments_last_run_date,sc.payments_last_run_date),
	replacement_attempts = coalesce(sub_final_migrated_dates.replacement_attempts,0)::int + coalesce(sc.replacement_attempts,0)::int,
	variant_sku = sc.variant_sku,
	is_bundle = sc.is_bundle,
	country_name = sc.country_name,
	store_commercial = sc.store_commercial,
	store_name = sc.store_name,
	store_label = sc.store_label,
	store_short = sc.store_short,
	account_name = sc.account_name,
	product_sku = sc.product_sku,
	product_name = sc.product_name,
	category_name = sc.category_name,
	subcategory_name = sc.subcategory_name,
	brand = sc.brand,
	trial_days = sc.trial_days,
	trial_variant = sc.trial_variant,
	renewal_date = sc.renewal_date,
	dc_status = sc.dc_status,
	result_debt_collection_contact = sc.result_debt_collection_contact,
	months_required_to_own = sc.months_required_to_own
from ods_subscription_legacy sc
where --sc.cancellation_note  ilike '%infra%' and 
sc.subscription_bo_id = sub_final_migrated_dates.subscription_id;

begin;

truncate table ods_production.subscription;

insert into ods_production.subscription
select
subscription_id,
subscription_name,
subscription_bo_id,
order_item_sf_id,
order_item_id,
rental_period,
subscription_plan,
store_id,
order_id,
customer_id,
subscription_value,
committed_sub_value,
currency,
subscription_value_euro,
reporting_subscription_value_euro,
start_date::timestamp as start_date,
first_asset_delivery_date,
asset_was_delivered,
status,
subscription_duration,
subscription_duration_in_months,
renewal_date,
next_renewal_date,
cancellation_requested_date,
cancellation_date::timestamp as cancellation_date,
cancellation_note,
cancellation_reason,
cancellation_reason_dropdown,
payment_method,
created_date,
updated_date,
allocation_status,
cross_sale_attempts,
manual_allocation_attempts,
allocation_tries,
minimum_cancellation_date,
days_to_cancel,
minimum_term_months,
order_product_name,
payments_last_run_date,
coeo_claim_date__c,
coeo_claim_id__c,
agency_for_dc_processing__c,
dc_agency_case_id__c,
dc_status,
debt_collection_handover_date::timestamp as debt_collection_handover_date,
result_debt_collection_contact,
reactivated_date,
replacement_attempts,
variant_sku,
is_bundle,
country_name,
store_commercial,
store_name,
store_label,
store_short,
account_name,
product_sku,
product_name,
category_name,
subcategory_name,
brand,
trial_days,
trial_variant,
case when months_required_to_own is null or months_required_to_own like '%e%'
or length(months_required_to_own)>=7 then null 
else months_required_to_own 
end as  months_required_to_own,
customer_acquisition_date,
rank_subscriptions,
subscriptions_per_customer,
migration_date,
buyout_disabled,
buyout_disabled_at,
buyout_disabled_reason,
state
from sub_final_migrated_dates;

commit;
