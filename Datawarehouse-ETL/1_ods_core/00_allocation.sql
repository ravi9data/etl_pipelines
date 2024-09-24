--find allocation's warehouse code based on asset
drop table if exists staging.asset_wh_codes;
create table staging.asset_wh_codes as 
with 
--assets without any wh change
--these assets' wh information cannot be found
--in asset history table of salesforce
never_changed as (
	select 
		created_date, 
		asset_id, 
		warehouse  
	from ods_production.asset 
	where asset_id not in 
		(select assetid 
		   from stg_salesforce.asset_history
		  where field ='warehouse__c'
		    and not (oldvalue='synerlogis_de' and newvalue in ('office_us', 'ups_softeon_us_kylse'))
		    )),
--find initial warehouse code of assets 
--whose wh code has changed at least once
--assume asset creation date as event timestamp
first_wh_pre as (
	select 
		assetid as asset_id ,
		oldvalue as warehouse,
		row_number() over (partition by asset_id order by createddate asc)	idx
	from stg_salesforce.asset_history
	 where field ='warehouse__c'
	  and not (oldvalue='synerlogis_de' and newvalue in ('office_us', 'ups_softeon_us_kylse'))
	  ),
first_wh as (
	select 
		a.created_date  , 
		ah.asset_id , 
		ah.warehouse
	from first_wh_pre ah 
	left join ods_production.asset a on 
	ah.asset_id = a.asset_id 
	where idx = 1
),
--get when warehouse information has changed
new_wh as (
	select 
		createddate as created_date,
		assetid as asset_id ,
		newvalue as warehouse
	  from stg_salesforce.asset_history
	 where field ='warehouse__c'
	   and not (oldvalue='synerlogis_de' and newvalue in ('office_us', 'ups_softeon_us_kylse'))
	 )
select created_date, asset_id, warehouse from never_changed 
union all 
select created_date, asset_id, warehouse from new_wh 
union all 
select created_date, asset_id, warehouse from first_wh ;


--
-- Get Replacment Allocations for New Infra Orders
--
drop table if exists replacement_allocated;
create temp table replacement_allocated as
select 
	allocation_uid ,
	order_id,
	customer_id,
	reservation_uid ,
	asset_id ,
	serial_number,
	store_id ,
	allocated_at ,
	nullif(warehouse, '') as warehouse,
	replaced_allocation_uid as replacement_for,
	replacement_date
from staging.spectrum_operations_replacement_allocated  
where salesforce_allocation_id = ''
qualify row_number () over (partition by allocation_uid order by allocated_at asc) = 1
--each replaced_allocation_uid should have only 1 allocation_uid, multiple replacements were created by mistake 
and row_number () over (partition by replaced_allocation_uid order by allocated_at desc) = 1;


--
-- Get Regular Allocations for New Infra Orders
-- Exclude allocations already existing in Replacement list
--
drop table if exists order_allocated;
create temp table order_allocated as 
select 
	allocation_uid ,
	order_id,
	customer_id,
	reservation_uid ,
	asset_id ,
	serial_number,
	store_id ,
	allocated_at ,
	nullif(warehouse, '') as warehouse
from staging.spectrum_operations_order_allocated 
where salesforce_allocation_id = ''
and allocation_uid not in (select allocation_uid from replacement_allocated)
qualify row_number () over (partition by allocation_uid order by allocated_at asc) = 1;


--
-- Merge both sources
--
drop table if exists new_infra_all_allocated;
create temp table new_infra_all_allocated as 
select 
	allocation_uid ,
	order_id ,
	customer_id ,
	reservation_uid ,
	asset_id ,
	serial_number,
	store_id ,
	allocated_at::timestamp,
	warehouse ,
	replacement_for::text,
	replacement_date::timestamp 
from replacement_allocated
union 
select 
	allocation_uid ,
	order_id ,
	customer_id ,
	reservation_uid ,
	asset_id ,
	serial_number,
	store_id ,
	allocated_at::timestamp,
	warehouse ,
	null::text as replacement_for,
	null::timestamp as replacement_date
from order_allocated ;


drop table if exists stg_kafka_events_full.allocation_us;
create table stg_kafka_events_full.allocation_us as 
with
revoked_contracts as (
	select 
		contract_id,
		max(case when event_name = 'revocation_started' then consumed_at end) as widerruf_claim_date,
		max(case when event_name = 'revocation_completed' then consumed_at end) as widerruf_validity_date
	from staging.customers_contracts 
	where event_name in ('revocation_started', 'revocation_completed') 
	group by 1
   )
   		select a.allocation_uid as allocation_id,
		NULL as allocation_sf_id,
        a.asset_id as asset_id,
        a.serial_number,
        a.customer_id as customer_id,
        a.order_id as order_id,
        so.contract_id as subscription_id,
        case when si.delivered_date is not null then 'RETURNED'
			when coalesce (si.shipped_date, si.exception_date, si.failed_delivery_at) is not null then 'IN TRANSIT'
			when coalesce (si.created_date, so.delivered_date) is not null then 'DELIVERED'
			when so.failed_delivery_at is not null then 'CANCELLED'
			when coalesce (so.exception_date, so.shipped_date) is not null then 'SHIPPED'
			when so.created_date is not null then 'READY TO SHIP'
			else 'PENDING SHIPMENT' end  as allocation_status_original,
        case when allocation_status_original in ('DELIVERED', 'RETURNED') 
			 then allocation_status_original
         	 else 'TRANSITION' end as status_new,
        --NULL as asset_status,
        a.allocated_at::timestamp as created_at,
        coalesce(si.updated_at,so.updated_at,a.allocated_at::timestamp) as updated_at,
        ord.submitted_date::timestamp as order_completed_at,
        ord.approved_date::timestamp as order_approved_at,
        NULL::timestamp as automatically_allocated_at,
        NULL as is_manual_allocation,
        false as is_bundle,
        --NULL as is_recirculated,
       -- NULL::bigint as rank_allocations_per_asset,
		--NULL::bigint as total_allocations_per_asset,
		 --false as is_last_allocation_per_asset,
		rank() OVER (PARTITION BY so.contract_id ORDER BY a.allocated_at, so.shipped_date DESC) AS rank_allocations_per_subscription,
		count(*) OVER (PARTITION BY so.contract_id) as total_allocations_per_subscription,
		a.allocated_at::timestamp as allocated_at, 
		so.shipping_profile as shipment_provider,
        NULL::text as shipcloud_profile,
    	so.shipping_profile as shipping_profile,
    	so.carrier as shipping_provider,
		so.package_id as wh_goods_order_id__c,
		so.created_date::timestamp as wh_goods_order__c,
		so.tracking_number  as shipment_tracking_number,
		so.tracking_id as shipment_id, --this will be a seperate column later
		so.created_date::timestamp as shipment_label_created_at,
		wh_goods_order__c as ready_to_ship_at,
		so.shipped_date::timestamp as picked_by_carrier_at,
		so.shipped_date::timestamp as shipment_at,
		so.failed_delivery_at::timestamp as failed_delivery_at,
		so.exception_date::timestamp as failed_delivery_candidate,
		so.delivered_date::timestamp as delivered_at,
		a.replacement_date::timestamp,
        r.allocation_uid as replaced_by,
        a.replacement_for,
		NULL as	replacement_reason,
		rc.widerruf_claim_date::timestamp as widerruf_claim_date,
        rc.widerruf_validity_date::timestamp as widerruf_validity_date,
        si.shipping_profile as return_shipment_provider,
        si.tracking_id as return_shipment_id, --this will be a seperate column later
        si.tracking_number as return_shipment_tracking_number,
        si.created_date::timestamp as return_shipment_label_created_at,
		si.shipped_date::timestamp as return_shipment_at,
		least(si.shipped_date, si.exception_date, si.failed_delivery_at)::timestamp as in_transit_at,
		si.delivered_date::timestamp as return_delivery_date,
		NULL::timestamp as return_delivery_date_old,
		NULL::timestamp as return_processed_at,
		NULL::timestamp as cancellation_requested_at,
		r.replacement_date::timestamp as cancellation_returned_at, --later IM, Synerlogis data will be implemented
		NULL::timestamp as cancellation_in_transit,
		NULL::timestamp as refurbishment_start_at,
		NULL::timestamp as refurbishment_end_at,
		NULL::timestamp as in_stock_at,
		FALSE as is_package_lost
		,NULL as initial_condition
		,NULL as initial_external_condition
		,NULL as initial_final_condition
		,NULL as initial_functional_condition
		,NULL as initial_package_condition
		,NULL as returned_condition
		,NULL as returned_external_condition
		,NULL as returned_external_condition_note
		,NULL as returned_final_condition
		,NULL as returned_functional_condition
		,NULL as returned_functional_condition_note
		,NULL as returned_package_condition
		,NULL as returned_package_condition_note
		,NULL as issue_reason
		,NULL::timestamp as issue_date
		,NULL as wh_feedback
		,NULL as issue_comments
		,account_name as account_name
		,t.product_sku  as product_sku
		,NULL::numeric as revenue_share_percentage
		,NULL as is_agreement
		,NULL::date as max_usage_date
		,NULL::int as days_on_rent
		,NULL::float8 as months_on_rent
		--,NULL::timestamp as goods_order_date
		--,false as is_weekend_wh_order
		--,false as is_picked
		--,false as is_target_sameday
		--,false as is_sla_met
		--,null::bigint as total_delivery_time_excl_weekend
		--,false as is_sla_met_synerlogis
		,coalesce(a.warehouse, 
					( select warehouse 
					  from staging.asset_wh_codes h
					  where a.asset_id = h.asset_id 
					  and date_trunc('day', a.allocated_at::timestamp) >= date_trunc('day', h.created_date::timestamp)
					  order by datediff ('minute', h.created_date::timestamp, a.allocated_at::timestamp) asc
					  limit 1 )
		) as warehouse
		from new_infra_all_allocated a   
		left join ods_production.asset t 
		on a.asset_id = t.asset_id
		left join replacement_allocated r 
                on a.allocation_uid = r.replacement_for
		left join staging.shipment_outbound so 
				on so.allocation_uid = a.allocation_uid
		--left join outbound_dates od 
		--	on so.allocation_uid = od.allocation_uid
		left join ods_production."order" ord 
		   on a.order_id = ord.order_id
		left join staging.shipment_inbound si 
			on si.allocation_uid = a.allocation_uid
		--left join inbound_dates id 
		--   on id.allocation_uid = i.allocation_uid
		left join ods_production.store s 
		   on a.store_id = s.id 
		--left join ods_production.variant v 
		--on v.variant_sku = a.variant_sku 
		--left join ods_production.product p 
		--on v.product_id = p.product_id 
		left join revoked_contracts rc 
		on rc.contract_id = so.contract_id
		;
		
			

--LEGACY infra

drop table if exists ods_production.allocation_old_infra;
create table ods_production.allocation_old_infra as
with 
allocation_asset as (
	 SELECT aa.id,
    		rank() OVER (PARTITION BY aa.asset__c ORDER BY aa.createddate, aa.allocated__c, aa.shipment_date__c DESC) AS rank_allocations_per_asset,
			  rank() OVER (PARTITION BY aa.subscription__c ORDER BY aa.createddate, aa.allocated__c, aa.shipment_date__c DESC) AS rank_allocations_per_subscription,
   		    count(*) OVER (PARTITION BY aa.asset__c) AS total_allocations_per_asset,
   		    count(*) OVER (PARTITION BY aa.subscription__c) AS total_allocations_per_subscription,
      		  CASE
         		   WHEN rank() OVER (PARTITION BY aa.asset__c ORDER BY aa.createddate, aa.allocated__c, aa.shipment_date__c DESC) = count(*) OVER (PARTITION BY aa.asset__c) THEN true
            		ELSE false
        	END AS is_last_allocation_per_asset
	FROM stg_salesforce.customer_asset_allocation__c aa
	  WHERE true AND (/*aa.subscription__c IS NOT NULL OR*/ aa.asset__c IS NOT NULL) 
  		AND (aa.status__c::text not in ('CANCELLED', 'TO BE FIXED') or aa.failed_delivery__c is not null)
  		),
bundles AS (
     SELECT aa_1.subscription__c,
          	COALESCE(count(DISTINCT a_1.f_product_sku_product__c), 0::bigint) AS products_per_subscription
      FROM stg_salesforce.customer_asset_allocation__c aa_1
         LEFT JOIN stg_salesforce.asset a_1 ON aa_1.asset__c::text = a_1.id::text
         GROUP BY aa_1.subscription__c
     		),
bundles_2 AS (
         SELECT DISTINCT s.subscription__c as subscription_id,
            count(DISTINCT s.allocation__c) AS allocations_per_sub
          FROM stg_salesforce.subscription_payment__c as s
          WHERE s.number__c = 2
          GROUP BY s.subscription__c
         HAVING count(DISTINCT s.allocation__c) > 1
        ),
ord AS (
         SELECT DISTINCT o.spree_order_number__c as order_id,
            coalesce(o.createddate,o.completed__c) AS order_completed_at,
            coalesce(o.approved__c,o.state_approved__c) AS order_approved_at
           FROM stg_salesforce."order" o
        )
select 
aa.id as allocation_id,
aa.name as allocation_sf_id,
a.id as asset_id,
a.serialnumber AS serial_number,
c.spree_customer_id__c as customer_id,
o.spree_order_number__c as order_id,
aa.subscription__c as subscription_id,
CASE WHEN aa.status__c in ('FAILED DELIVERY', 'TO BE REPLACED', 'TO BE FIXED')  
	 THEN 'CANCELLED' --OPS people do these manually, they are simply Cancelled , no longer valid
	 ELSE aa.status__c end as allocation_status_original,
	CASE
         WHEN aa.status__c::text in('FAILED DELIVERY', 'TO BE REPLACED','PENDING SHIPMENT', 'DROPSHIPPED', 'IN TRANSIT', 'READY TO SHIP', 'SHIPPED') THEN 'TRANSITION'
         ELSE aa.status__c
        END AS status_new,
--aa.asset_status__c as asset_status,        
aa.createddate as created_at,
greatest(aa.lastmodifieddate,aa.systemmodstamp) as updated_at,
ord.order_completed_at,
ord.order_approved_at,
aa.automatically_allocated__c as automatically_allocated_at,
CASE WHEN aa.automatically_allocated__c IS NOT NULL THEN 'automatic'::text ELSE 'manual'::text END AS is_manual_allocation,
CASE WHEN b.products_per_subscription > 1 AND b2.subscription_id IS NOT NULL THEN true ELSE false END AS is_bundle,
--CASE WHEN aa2.rank_allocations_per_asset = 1 OR aa.initial_final_condition__c::text = 'NEW'::text THEN 'New'::text ELSE 'Re-circulated'::text END AS is_recirculated,
--aa2.rank_allocations_per_asset, 
--aa2.total_allocations_per_asset,
--aa2.is_last_allocation_per_asset,
aa2.rank_allocations_per_subscription,
aa2.total_allocations_per_subscription,
least(aa.allocated__c,aa.createddate) as allocated_at,
--aa.shipment_date__c as shipment_at,
--coalesce(aa.shipping_profile__c,aa.shipcloud_profile__c,aa.shipping_provider__c) as shipment_provider,
case when coalesce(aa.shipping_provider__c, aa.shipcloud_profile__c) is null or 
		 	  split_part(aa.shipping_profile__c, ' ', 1) = split_part( coalesce(aa.shipping_provider__c , aa.shipcloud_profile__c ), ' ', 1)
		 then aa.shipping_profile__c 
		 else coalesce(aa.shipping_provider__c , aa.shipcloud_profile__c )
	end as shipment_provider,
aa.shipcloud_profile__c as shipcloud_profile,
aa.shipping_profile__c as shipping_profile,
aa.shipping_provider__c as shipping_provider,
aa.wh_goods_order_id__c,
aa.wh_goods_order__c,
aa.shipment_tracking_number__c as shipment_tracking_number,
case when aa.shipcloud_shipment_id__c like '%-%' then right(aa.tracking_url__c,40) 
	when aa.tracking_url__c like '%myhes.de%' then right(aa.tracking_url__c, len(aa.tracking_url__c) - 29)
	else aa.shipcloud_shipment_id__c end as shipment_id,
aa.shipping_label_created__c as shipment_label_created_at, 
COALESCE(aa.ready_to_ship__c, aa.shipping_label_created__c, aa.picked_by_carrier__c, aa.shipment_date__c) AS ready_to_ship_at,
aa.picked_by_carrier__c as picked_by_carrier_at,  
COALESCE(aa.picked_by_carrier__c, aa.shipment_date__c) AS shipment_at,    
aa.failed_delivery__c as failed_delivery_at,
fd.failed_timestamp as failed_delivery_candidate,
case when total_allocations_per_subscription = 1 
then  (case when aa.return_picked_by_carrier__c is not null
      then coalesce(aa.delivered__c, s.date_first_asset_delivery__c,aa.picked_by_carrier__c, aa.shipment_date__c)
	  else 
	  coalesce(aa.delivered__c,s.date_first_asset_delivery__c)
	  End)
     when total_allocations_per_subscription > 1
      then (case when aa.return_picked_by_carrier__c is not null
      then coalesce(aa.delivered__c,aa.picked_by_carrier__c, aa.shipment_date__c)
	  else
	  coalesce(aa.delivered__c)	
      End) else coalesce(aa.delivered__c)
end as delivered_at,
aa.replacement_date__c as replacement_date,
aa.replaced_by__c as replaced_by,
aa.replacement_for__c as replacement_for,
aa.replacement_reason__c as replacement_reason,
aa.widerruf_claim_date__c as widerruf_claim_date,
aa.widerruf_validity_date__c as widerruf_validity_date,
aa.return_shipment_provider__c as return_shipment_provider,
aa.shipcloud_return_shipment_id__c as return_shipment_id,
aa.return_tracking_number__c as return_shipment_tracking_number,
aa.return_label_created__c as return_shipment_label_created_at,
--aa.return_picked_by_carrier__c as return_picked_by_carrier_at,
COALESCE(aa.return_picked_by_carrier__c, aa.cancelltion_in_transit__c) AS return_shipment_at,
aa.cancelltion_in_transit__c as in_transit_at,
--aa.return_delivered__c as return_delivered_at,
--COALESCE(aa.return_delivered__c, aa.cancelltion_returned__c) AS return_delivery_date,
case when delivered_at is not null then
 case when aa2.is_last_allocation_per_asset = false 
 and COALESCE(aa.return_delivered__c, aa.cancelltion_returned__c) is null 
  then greatest(aa.subscription_cancellation__c,aa.cancelltion_in_transit__c,aa.return_picked_by_carrier__c)
   else COALESCE(aa.return_delivered__c, aa.cancelltion_returned__c) 
   end
   end as return_delivery_date,
 case when aa2.is_last_allocation_per_asset = false 
 and COALESCE(aa.return_delivered__c, aa.cancelltion_returned__c) is null 
  then greatest(aa.subscription_cancellation__c,aa.cancelltion_in_transit__c,aa.return_picked_by_carrier__c)
   else COALESCE(aa.return_delivered__c, aa.cancelltion_returned__c) 
   end as return_delivery_date_old,
s.date_cancellation__c as return_processed_at,
aa.cancelltion_requested__c as cancellation_requested_at,
aa.cancelltion_returned__c as cancellation_returned_at,
aa.cancelltion_in_transit__c as cancellation_in_transit,
COALESCE(aa.cancelltion_returned__c, aa.cancelltion_approved__c) AS refurbishment_start_at,
--aa.cancelltion_approved__c as cancellaion_approved_at,
aa.cancelltion_approved__c as refurbishment_end_at,
COALESCE(
        CASE
            WHEN aa.status__c::text = 'SOLD'::text THEN NULL::timestamp
            ELSE COALESCE(aa.cancelltion_approved__c, aa.subscription_cancellation__c)
        END, lead(aa.allocated__c) OVER (PARTITION BY aa.asset__c ORDER BY aa.allocated__c)) AS in_stock_at,
aa.package_lost__c as is_package_lost,
initial_condition__c as initial_condition, 
initial_external_condition__c as initial_external_condition,
initial_final_condition__c as initial_final_condition,
initial_functional_condition__c as initial_functional_condition, 
initial_package_condition__c as initial_package_condition,
returned_condition__c as returned_condition,
returned_external_condition__c as returned_external_condition,
returned_external_condition_note__c as returned_external_condition_note, 
returned_final_condition__c as returned_final_condition,
returned_functional_condition__c as returned_functional_condition,
returned_functional_condition_note__c as returned_functional_condition_note,
returned_package_condition__c as returned_package_condition,
returned_package_condition_note__c as returned_package_condition_note,
aa.reported_issue_reason__c AS issue_reason,
aa.issue_report_date__c AS issue_date,
aa.wh_feedback__c AS WH_feedback,
aa.issue_report_comments__c AS issue_comments,
st.account_name,
a.f_product_sku_product__c as product_sku,
 CASE
            WHEN st.account_name='Media Markt' 
             AND least(aa.allocated__c,aa.createddate) >= '2018-09-17 00:00:00'::timestamp without time zone 
              THEN 0.06::numeric(6,3)
              WHEN st.account_name='Saturn' 
             AND least(aa.allocated__c,aa.createddate) >= '2018-09-17 00:00:00'::timestamp without time zone 
              THEN 0.06::numeric(6,3)
            WHEN st.account_name='Conrad' 
             THEN 0.03::numeric(6,3)
            WHEN st.account_name='Gravis' 
             THEN 0.03::numeric(6,3)
            WHEN a.f_product_sku_product__c::text in ('GRB211P2340', 'GRB211P2338', 'GRB122P1799', 'GRB122P1801', 'GRB211P1802', 'GRB122P1800', 'GRB211P1803') 
             THEN 0.7::numeric(6,3)
            WHEN a.f_product_sku_product__c::text = 'GRB129P174' 
             THEN 0.65::numeric(6,3)
            WHEN st.account_name='Thalia' OR su.supplier_account in ('Thalia')
             THEN 0.7::numeric(6,3)
             WHEN st.account_name='Aldi Talk'
             THEN 0.03::numeric(6,3)
             WHEN st.account_name='Weltbild'
             THEN 0.03::numeric(6,3)
               WHEN st.account_name='UNITO'
             THEN 0.03::numeric(6,3)
             WHEN st.account_name='Comspot'
             THEN 0.03::numeric(6,3)
             WHEN st.account_name='Shifter'
             THEN 0.03::numeric(6,3)
            ELSE rs.revenue_share_percentage::numeric(6,3)
        END AS revenue_share_percentage,
   CASE
            WHEN st.account_name='Media Markt' 
             AND least(aa.allocated__c,aa.createddate) >= '2018-09-17 00:00:00'::timestamp without time zone 
              THEN 'Re-circulation agreement'::text
              WHEN st.account_name='Saturn' 
             AND least(aa.allocated__c,aa.createddate) >= '2018-09-17 00:00:00'::timestamp without time zone 
              THEN 'Re-circulation agreement'::text
            WHEN st.account_name='Conrad' 
             THEN 'Re-circulation agreement'::text
            WHEN (st.store_name in ('Qbo', 'Tchibo')) 
             OR (su.supplier_account in ('Tchibo')) 
              AND st.store_name = 'Germany' THEN 'Re-circulation agreement'
            WHEN st.account_name='Gravis'::text 
             THEN 'Re-circulation agreement'::text
            WHEN su.supplier_account in ('Thalia') 
             OR lower(st.store_name) like '%thalia%' 
              THEN 'Re-circulation agreement'
            WHEN (st.account_name='Media Markt' OR st.account_name='Saturn') 
             AND rs.revenue_share_percentage IS NOT NULL 
              THEN 'Re-circulation agreement'::text
               WHEN (st.account_name='Aldi Talk' OR st.account_name='Weltbild' or st.account_name='UNITO' or st.account_name='Comspot' or  st.account_name='Shifter') 
              THEN 'Re-circulation agreement'::text
            WHEN (st.account_name='Media Markt' or st.account_name='Saturn') 
             AND (b.products_per_subscription > 1 AND b2.subscription_id IS NOT NULL) 
             AND rs.revenue_share_percentage IS NULL 
             AND max(rs.revenue_share_percentage) OVER (PARTITION BY s.name) > 0::numeric 
              THEN 'Bundle Re-circulation'::text
            ELSE 'No agreement'::text
        END AS is_agreement,
        coalesce(cancellation_returned_at::date,return_processed_at::date,return_delivery_date::date,CURRENT_DATE) as max_usage_date,
        (case when delivered_at is not null then 
	case when total_allocations_per_subscription = 1
	then
(coalesce(cancellation_returned_at::date,return_processed_at::date,return_delivery_date::date,CURRENT_DATE+1)
								-((delivered_at::date)))
		else 
(coalesce(cancellation_returned_at::date,return_processed_at::date,return_delivery_date::date,CURRENT_DATE+1)
								-((delivered_at::date))) end			
								else 0 end) as days_on_rent,
case when delivered_at is not null	then 	
	case when DATE_PART('year',max_usage_date)	>= DATE_PART('year',delivered_at::date)
	            then case when DATE_PART('month',max_usage_date) >= DATE_PART('month',delivered_at::date)
	            			then case when DATE_PART('day',max_usage_date) >= DATE_PART('day',delivered_at::date)
	            			          then ((DATE_PART('year',max_usage_date) - DATE_PART('year',delivered_at::date)) * 12) + (DATE_PART('month',max_usage_date) - DATE_PART('month',delivered_at::date))+1
	            			           else 
		            			       ((DATE_PART('year',max_usage_date) - DATE_PART('year',delivered_at::date)) * 12) + (DATE_PART('month',max_usage_date) - DATE_PART('month',delivered_at::date)) end	  
	                 else 
		                        case when DATE_PART('day',max_usage_date) >= DATE_PART('day',delivered_at::date) 
		                        then ((DATE_PART('year',max_usage_date) - DATE_PART('year',delivered_at::date)) * 12) - (DATE_PART('month',delivered_at::date)-DATE_PART('month',max_usage_date))+1
		                         else
			                       ((DATE_PART('year',max_usage_date) - DATE_PART('year',delivered_at::date)) * 12) - (DATE_PART('month',delivered_at::date)-DATE_PART('month',max_usage_date)) end
			         end
		 end 
  else 0 end   as months_on_rent,
  --we calculate this in weekly/monthly shipment scripts
--(DATEDIFF('day', order_completed_at::timestamp, delivered_at::timestamp ) - datediff('week', order_completed_at::timestamp, delivered_at::timestamp ))   as total_delivery_time_excl_weekend, 
coalesce(oa.warehouse, 
			( select warehouse 
			 from staging.asset_wh_codes h
			 where a.id = h.asset_id 
			   and date_trunc('day', least(aa.allocated__c,aa.createddate)::timestamp) >= date_trunc('day', h.created_date::timestamp)
	 		order by datediff ('minute', h.created_date::timestamp, least(aa.allocated__c,aa.createddate)::timestamp) asc
	 	    limit 1 )
 ) as warehouse	                   
from stg_salesforce.customer_asset_allocation__c aa 
left join stg_salesforce.account c 
 on aa.customer__c=c.id
left join stg_salesforce.asset a 
 on aa.asset__c=a.id
left join ods_production.supplier su
 on su.supplier_id=a.supplier__c 
left join stg_salesforce.subscription__c s 
 on s.id::text=aa.subscription__c::text
left join stg_salesforce.order o 
 on o.id=aa.order__c
left join bundles b 
 ON aa.subscription__c::text = b.subscription__c::text
left join bundles_2 b2 
 ON aa.subscription__c::text = b2.subscription_id::text
LEFT JOIN ord  
 ON ord.order_id::text = o.spree_order_number__c::text
left JOIN allocation_asset aa2 
 ON aa.id::text = aa2.id::text
left join ods_production.store st 
 ON st.id=o.store_id__c
left join trans_dev.media_markt_revenue_share_percentage rs 
 on rs.product_sku = a.f_product_sku_product__c 
 AND (st.store_name = 'Media Markt' OR st.store_name = 'Saturn') 
left join ods_operations.failed_deliveries fd 
 on fd.tracking_id = case when aa.shipcloud_shipment_id__c like '%-%' then right(aa.tracking_url__c,40) 
						  when aa.tracking_url__c like '%myhes.de%' then right(aa.tracking_url__c, len(aa.tracking_url__c) - 29)
	                      else aa.shipcloud_shipment_id__c end 
left join stg_kafka_events_full.order_allocated oa 
 on oa.salesforce_allocation_id = aa.id;

DROP Table if exists allocation_new_old_pre_merge;
CREATE TEMP TABLE allocation_new_old_pre_merge as 
	with union_a as (
		select 
	 	allocation_id,
		allocation_sf_id,
		asset_id,
		serial_number,
		customer_id,
		order_id,
		subscription_id,
		allocation_status_original,
		status_new,
		created_at,
		updated_at,
		order_completed_at,
		order_approved_at,
		automatically_allocated_at,
		is_manual_allocation,
		is_bundle,
		rank_allocations_per_subscription,
		total_allocations_per_subscription,
		allocated_at,
		shipment_provider,
		shipcloud_profile,
		shipping_profile,
		shipping_provider ,
		wh_goods_order_id__c,
		wh_goods_order__c,
		shipment_tracking_number,
		shipment_id,
		shipment_label_created_at,
		ready_to_ship_at,
		picked_by_carrier_at,
		shipment_at,
		failed_delivery_at,
		failed_delivery_candidate,
		delivered_at,
		replacement_date,
		replaced_by,
		replacement_for,
		replacement_reason,
		widerruf_claim_date,
		widerruf_validity_date,
		return_shipment_provider,
		return_shipment_id,
		return_shipment_tracking_number,
		return_shipment_label_created_at,
		return_shipment_at,
		in_transit_at,
		return_delivery_date,
		return_delivery_date_old,
		return_processed_at,
		cancellation_requested_at,
		cancellation_returned_at,
		cancellation_in_transit,
		refurbishment_start_at,
		refurbishment_end_at,
		in_stock_at,
		is_package_lost,
		initial_condition,
		initial_external_condition,
		initial_final_condition,
		initial_functional_condition,
		initial_package_condition,
		returned_condition,
		returned_external_condition,
		returned_external_condition_note,
		returned_final_condition,
		returned_functional_condition,
		returned_functional_condition_note,
		returned_package_condition,
		returned_package_condition_note,
		issue_reason,
		issue_date,
		wh_feedback,
		issue_comments,
		account_name,
		product_sku,
		revenue_share_percentage,
		is_agreement,
		max_usage_date,
		days_on_rent,
		months_on_rent,
		warehouse
		from stg_kafka_events_full.allocation_us
		union all 
 		select 
		allocation_id,
		allocation_sf_id,
		asset_id,
		serial_number,
		customer_id,
		order_id,
		subscription_id,
		allocation_status_original,
		status_new,
		created_at,
		updated_at,
		order_completed_at,
		order_approved_at,
		automatically_allocated_at,
		is_manual_allocation,
		is_bundle,
		rank_allocations_per_subscription,
		total_allocations_per_subscription,
		allocated_at,
		shipment_provider,
		shipcloud_profile,
		shipping_profile,
		shipping_provider ,
		wh_goods_order_id__c,
		wh_goods_order__c,
		shipment_tracking_number,
		shipment_id,
		shipment_label_created_at,
		ready_to_ship_at,
		picked_by_carrier_at,
		shipment_at,
		failed_delivery_at,
		failed_delivery_candidate,
		delivered_at,
		replacement_date,
		replaced_by,
		replacement_for,
		replacement_reason,
		widerruf_claim_date,
		widerruf_validity_date,
		return_shipment_provider,
		return_shipment_id,
		return_shipment_tracking_number,
		return_shipment_label_created_at,
		return_shipment_at,
		in_transit_at,
		return_delivery_date,
		return_delivery_date_old,
		return_processed_at,
		cancellation_requested_at,
		cancellation_returned_at,
		cancellation_in_transit,
		refurbishment_start_at,
		refurbishment_end_at,
		in_stock_at,
		is_package_lost,
		initial_condition,
		initial_external_condition,
		initial_final_condition,
		initial_functional_condition,
		initial_package_condition,
		returned_condition,
		returned_external_condition,
		returned_external_condition_note,
		returned_final_condition,
		returned_functional_condition,
		returned_functional_condition_note,
		returned_package_condition,
		returned_package_condition_note,
		issue_reason,
		issue_date,
		wh_feedback,
		issue_comments,
		account_name,
		product_sku,
		revenue_share_percentage,
		is_agreement,
		max_usage_date,
		days_on_rent,
		months_on_rent,
		warehouse
 		from ods_production.allocation_old_infra
	)
	select 
		u.*, 
		--re-circulated calculation is applied only if asset is delivered
		case when allocation_status_original not in ('CANCELLED','TO BE FIXED')
				  or failed_delivery_at is not null 
			 then 1 end _cnt,
		count(_cnt) over (partition by u.asset_id) as total_allocations_per_asset,
		case when _cnt = 1 
			 then rank() over (partition by u.asset_id, _cnt order by allocated_at, shipment_at desc)  
		end rank_allocations_per_asset,
		case when rank_allocations_per_asset = 1 
				  or u.initial_final_condition::text = 'NEW'::text 
			 then 'New'::text 
			 else 'Re-circulated'::text 
		end as is_recirculated,
		case when total_allocations_per_asset = rank_allocations_per_asset 
			 then true else false 
		end as is_last_allocation_per_asset,
		--find the latest event to check asset status at that time
		greatest (	allocated_at, 
					picked_by_carrier_at,
					delivered_at,
					return_shipment_at,
					return_delivery_date,
					return_delivery_date_old,
					refurbishment_start_at,
					refurbishment_end_at,
			 		replacement_date ) greatest_at,
		--get current asset status for most recent allocation
		case when is_last_allocation_per_asset 
				  --also consider if last and only allocation is cancelled or to-be-fixed
				  --since we exclude these allocation for re-circulation calculation
				  or total_allocations_per_asset = 0
			 then t.status 
			 else coalesce (
					 --get the last status when allocation is last updated
					 (select ah.newvalue  
					 from stg_salesforce.asset_history ah
					 where ah.assetid = u.asset_id 
					 and greatest_at >= ah.createddate
					 and field = 'Status'
					 order by createddate desc 
					 limit 1),
					 --if still no data, then assign manually
					 case when allocation_status_original in ('TO BE FIXED',
					 					'TO BE REPLACED',
					 					'PENDING SHIPMENT',
					 					'SHIPPED',
					 					'DROPSHIPPED')
					 	  then 'RESERVED'
					 	  when allocation_status_original in ('IN TRANSIT')
					 	  then 'ON LOAN'
					 	  when allocation_status_original in ('FAILED DELIVERY',
					 	  					'DELIVERED',
					 	  					'RETURNED')
					 	  then 'RETURNED'
					 	  else 'RETURNED'
					 end )
	 end asset_status
	from union_a u 
	left join stg_salesforce.asset t 
	on u.asset_id = t.id ;
	
--this logic looks if this asset was returned or in stock at any time after order was delivered
DROP TABLE IF EXISTS new_asset_delivery_return_logic;
CREATE TEMP TABLE new_asset_delivery_return_logic AS
SELECT assetid AS asset_id,
       createddate AS returned_to_wh 
FROM stg_salesforce.asset_history ah
WHERE newvalue = 'IN STOCK'
   OR newvalue = 'RETURNED'; 

--as there are multiple rows for some allocations I am taking first one only as allocation_id should be unique
DROP TABLE IF EXISTS new_allocation_delivery_return_logic;
CREATE TEMP TABLE new_allocation_delivery_return_logic AS
SELECT allocation_id,
       return_event AS return_time,
       ROW_NUMBER() OVER (PARTITION BY allocation_id ORDER BY return_event ASC) AS rn
FROM ods_operations.ingram_allocation_mapping;

--here we are using only TRUE logic match so we look for cancelled and delivered orders without return date
DROP TABLE IF EXISTS tmp_allocation_return_delivery_mapping; 
CREATE TEMP TABLE tmp_allocation_return_delivery_mapping AS
SELECT
    a.allocation_id,
    a.asset_id,
    a.serial_number,
    a.customer_id,
    a.order_id,
    a.subscription_id,
    a.delivered_at,
    b.status,
    c.returned_to_wh,
    case when a.delivered_at  <= c.returned_to_wh
        AND a.delivered_at IS NOT NULL
        AND b.status = 'CANCELLED'
             THEN TRUE
        END AS logic_match,
    ROW_NUMBER() OVER (PARTITION BY a.asset_id, a.allocation_id, logic_match ORDER BY c.returned_to_wh ASC) AS rn
FROM allocation_new_old_pre_merge a
         LEFT JOIN ods_production.subscription b
                   ON a.subscription_id = b.subscription_id
         LEFT JOIN new_asset_delivery_return_logic c 
                   ON a.asset_id = c.asset_id;

--final select
--specificy each column instead of *
--for both efficiency and column sorting
drop table if exists ods_production.allocation;
create table ods_production.allocation as 
select distinct
	a.allocation_id,
	a.allocation_sf_id,
	a.asset_id,
	a.serial_number,
	a.customer_id,
	a.order_id,
	a.subscription_id,
	--for new infra, alter allocation status 
	--per asset status if sold or lost
	case when a.allocation_sf_id is null then
		case when a.asset_status = 'LOST' then 'CANCELLED'
			 when a.asset_status = 'SOLD' then 'SOLD' --aligned with SF logic
			 when a.cancellation_returned_at is not null then 'CANCELLED' --this also covers replacements t
			 else a.allocation_status_original
		end
		when b.returned_to_wh IS NOT NULL 
		  OR c.return_time IS NOT NULL THEN 'RETURNED'
		else a.allocation_status_original
	end as allocation_status_original,
	CASE WHEN b.returned_to_wh IS NOT NULL 
		  OR c.return_time IS NOT NULL THEN 'RETURNED' 
	ELSE a.status_new END AS status_new,
	a.asset_status,
	a.created_at,
	a.updated_at,
	a.order_completed_at,
	a.order_approved_at,
	a.automatically_allocated_at,
	a.is_manual_allocation,
	a.is_bundle,
	a.rank_allocations_per_subscription,
	a.total_allocations_per_subscription,
	a.allocated_at,
	a.shipment_provider,
	a.shipcloud_profile,
	a.shipping_profile,
	a.shipping_provider,
	a.wh_goods_order_id__c,
	a.wh_goods_order__c,
	a.shipment_tracking_number,
	a.shipment_id,
	a.shipment_label_created_at,
	a.ready_to_ship_at,
	a.picked_by_carrier_at,
	a.shipment_at,
	a.failed_delivery_at,
	a.failed_delivery_candidate,
	a.delivered_at,
	a.replacement_date,
	a.replaced_by,
	a.replacement_for,
	a.replacement_reason,
	a.widerruf_claim_date,
	a.widerruf_validity_date,
	a.return_shipment_provider,
	a.return_shipment_id,
	a.return_shipment_tracking_number,
	a.return_shipment_label_created_at,
	a.return_shipment_at,
	a.in_transit_at,
	COALESCE(a.return_delivery_date, b.returned_to_wh, c.return_time) AS return_delivery_date,
	a.return_delivery_date_old,
	a.return_processed_at,
	a.cancellation_requested_at,
	a.cancellation_returned_at,
	a.cancellation_in_transit,
	a.refurbishment_start_at,
	a.refurbishment_end_at,
	a.in_stock_at,
	case when a.allocation_sf_id is null 
			  and a.asset_status = 'LOST'
		 then true 
		 else a.is_package_lost end as is_package_lost,
	a.initial_condition,
	a.initial_external_condition,
	a.initial_final_condition,
	a.initial_functional_condition,
	a.initial_package_condition,
	a.returned_condition,
	a.returned_external_condition,
	a.returned_external_condition_note,
	a.returned_final_condition,
	a.returned_functional_condition,
	a.returned_functional_condition_note,
	a.returned_package_condition,
	a.returned_package_condition_note,
	a.issue_reason,
	a.issue_date,
	a.wh_feedback,
	a.issue_comments,
	a.account_name,
	a.product_sku,
	a.revenue_share_percentage,
	a.is_agreement,
	a.max_usage_date,
	a.days_on_rent,
	a.months_on_rent,
	--total_delivery_time_excl_weekend, --we calculate this in weekly/monthly shipment scripts
	a.warehouse,
	a.total_allocations_per_asset,
	a.rank_allocations_per_asset,
	a.is_recirculated,
	a.is_last_allocation_per_asset
from allocation_new_old_pre_merge a
   LEFT JOIN tmp_allocation_return_delivery_mapping b
      ON a.allocation_id = b.allocation_id
          AND b.logic_match = TRUE AND b.rn = 1
   LEFT JOIN new_allocation_delivery_return_logic c
      ON c.allocation_id = a.allocation_id AND c.rn = 1;

GRANT SELECT ON ods_production.allocation TO GROUP recommerce;
GRANT SELECT ON ods_production.allocation TO tableau,airflow_recommerce;
GRANT SELECT ON ods_production.allocation_old_infra TO tableau;
