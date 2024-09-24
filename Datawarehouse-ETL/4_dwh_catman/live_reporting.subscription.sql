							
DROP TABLE IF EXISTS tmp_live_reporting_subscription;
CREATE TEMP TABLE tmp_live_reporting_subscription AS 
with stg_kafka_v2 as
	(
select
		event_timestamp,
		event_name,
		version,
		payload,
		nullif(JSON_EXTRACT_PATH_text(payload,'id'),'')as id,
		nullif(JSON_EXTRACT_PATH_text(payload,'type'),'')as type,
		nullif(JSON_EXTRACT_PATH_text(payload,'user_id'),'')as user_id,
		nullif(JSON_EXTRACT_PATH_text(payload,'order_number'),'')as order_number,
		nullif(JSON_EXTRACT_PATH_text(payload,'state'),'')as state,
		nullif(JSON_EXTRACT_PATH_text(payload,'created_at'),'')as created_at,
		count(*) over (partition by id) as total_events,
		nullif(JSON_EXTRACT_PATH_text(payload,'goods'),'')as goods,
		nullif(JSON_EXTRACT_PATH_text((json_extract_array_element_text(goods,0)),'variant_sku'),'')as variant_sku,
		nullif(case when event_name = 'extended'
				then json_extract_path_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'duration_terms'),'new'), 'committed_length')
				else JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'duration_terms'), 'committed_length') end,'') as committed_length,
		row_number () over (partition by id order by event_timestamp desc) as idx
	from  stg_kafka_events_full.stream_customers_contracts_v2
	)
	,sub_dates as
	(
	select
		id as subscription_id,
		max(case when state = 'fulfilling' then event_timestamp::timestamp end) as start_date,
		min(case when state = 'active' then event_timestamp::timestamp end) as active_date,
		max(case when state = 'active' then event_timestamp::timestamp end) as reactivated_date,
		max(case when state in ('cancelled','ended','refused') then event_timestamp::timestamp end) as cancellation_date,
		max(case when state = 'paused' then event_timestamp::timestamp end) as paused_date
	from stg_kafka_v2
  --where type ='flex'
	group by 1
	)
    ,idx as (
	select skv.*,
	ROW_NUMBER() over (partition by order_number,variant_sku order by created_at asc) as c_idx
	from stg_kafka_v2 skv
		left join  ods_production.contracts_deleted_in_source cds
		on cds.contract_id = skv.id
	where
		idx = 1
		and cds.id is null /*exclude contracts deleted in source system */
	)
	,last_event_pre as
	(
	select
		le.*
		--,is_pay_by_invoice
	from idx le
	left join live_reporting.ORDER o
	on le.order_number = o.order_id
	--where (o.paid_date is not null
	--((is_pay_by_invoice is true and state = 'active') or
	-- (is_pay_by_invoice is true and state in('ended', 'cancelled'))  or
	-- (is_pay_by_invoice is true and state = 'paused'))
	--)
	)
	,res_agg
		as
			(select distinct * from  stg_kafka_events_full.stream_inventory_reservation
			)
		,res
		as (
		select
			row_number() over (partition by order_id,variant_sku order by updated_at ASC) as idx
			,*
			from res_agg
		where  ( order_mode = 'flex'
		and ((initial_quantity::int > quantity::int and status = 'paid')
		or status = 'fulfilled')
			)
		)
		,last_event as (
	select l.*,
	case when r.order_id is not null then true else false end is_fulfilled
	from last_event_pre l
	left join res r on l.order_number = r.order_id and l.c_idx = r.idx  and l.variant_sku = r.variant_sku)

select distinct
	--store_id,
	s.order_number as order_id,
	s.variant_sku as variant_sku,
	case when p.product_sku is not null then p.product_sku else split_part(s.variant_sku,'V',1) end as product_sku,
case
		when is_fulfilled is true then 'ALLOCATED'
		when aa.allocation_status_original is not null then 'ALLOCATED'
		when (active_date is not null and start_date < '2021-10-01') then 'ALLOCATED'
		else 'PENDING ALLOCATION'
	end as allocation_status,
	case
		when active_date is null and reactivated_date is null and cancellation_date is not null then 'CANCELLED'
		when cancellation_date >= active_date and reactivated_date is null then 'CANCELLED'
		when cancellation_date >= reactivated_date then 'CANCELLED'
		when cancellation_date is null then 'ACTIVE'
		when active_date is null and reactivated_date is null and cancellation_date is null then 'ACTIVE'
		else 'ACTIVE' end  as status,
		start_date::TIMESTAMP WITHOUT TIME ZONE ,
		product_name,
		committed_length::DOUBLE PRECISION as rental_period
	from
	last_event s
	left join sub_dates sd
	on s.id = sd.subscription_id
	left join stg_kafka_events_full.allocation_us aa
    on s.id = aa.subscription_id
	left join bi_ods.variant v 
	on s.variant_sku = v.variant_sku
	left join bi_ods.product p
	on p.product_id = v.product_id or (v.product_id is null and split_part(s.variant_sku,'V',1) = p.product_sku)
   left join ods_production.new_infra_missing_history_months_required h
--> this is a static table and won't generate locks.
	on s.id =  h.contract_id
union all
--subs old infra
select distinct
   -- cast(o.store_id__c as VARCHAR) as store_id,
    o.spree_order_number__c as order_id,
    i.f_product_sku_variant__c as variant_sku,
   case when p.product_sku is not null then p.product_sku else split_part(i.f_product_sku_variant__c,'V',1) end as product_sku,
 -- cast(c.spree_customer_id__c as integer) as customer_id,
s.allocation_status__c as allocation_status,
case when s.date_cancellation__c is not null then 'CANCELLED' else 'ACTIVE' end as status,
s.date_start__c::TIMESTAMP WITHOUT TIME ZONE  as start_date,
product_name,
case when coalesce(i.minimum_term_months__c,0)::int = 0 then 1 else coalesce(i.minimum_term_months__c,1) end as rental_period
FROM stg_salesforce.subscription__c s
left join stg_salesforce.orderitem i
   on s.order_product__c = i.id
 left join stg_salesforce.account c
   on c.id = s.customer__c
 left join stg_salesforce.ORDER o
   on s.order__c = o.id
  LEFT JOIN bi_ods.variant v 
   ON v.VARIANT_SKU=i.f_product_sku_variant__c
  LEFT JOIN bi_ods.product p
   ON p.product_id=v.product_id or (v.product_id is null and split_part(i.f_product_sku_variant__c,'V',1) = p.product_sku)
 ;

BEGIN TRANSACTION;

DELETE FROM live_reporting.subscription WHERE 1=1;

INSERT INTO live_reporting.subscription
SELECT * FROM tmp_live_reporting_subscription;

END TRANSACTION;