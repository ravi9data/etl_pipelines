	drop table if exists ods_production.mix_reporting;
   					create table ods_production.mix_reporting as
							with traffic_temp as (
					select
						*
					from
						scratch.se_events_flat se
					where
						se_category = 'productDiscovery'
						and se_action = 'pdpView'
						and is_mix = 'true' )
							,traffic as (
					select  distinct domain_userid as entity_id,
					collector_tstamp::date as entity_date,
					'Traffic' as kpi_type
					from traffic_temp)
					 		,add_to_cart_all as (
					select
							*
					from scratch.se_events_flat
					where se_action = 'addToCart'
					and is_mix = 'true')
							,add_to_cart as (
					select distinct order_id as entity_id,
					max(collector_tstamp::date) over (partition by order_id order by collector_tstamp rows between unbounded preceding and unbounded following) as entity_date,
					'Cart' as kpi_type
					from add_to_cart_all)
							,order_submitted as (
					select order_number as entity_id,
					 cast(timestamp 'epoch' + (published_at::bigint/1000) * interval '1 second'as date) as entity_date,
					'Submitted' as kpi_type
						from stg_kafka_events_full.stream_internal_order_placed where order_mode = 'MIX'
					)
							,order_approved as (
					select order_number as entity_id,
					event_timestamp::date as entity_date,
					'Approved' as kpi_type
					from stg_kafka_events_full.stream_internal_risk_order_decisions_v3
					where decision = 'approve'
					)
							,paid_orders as (
					 select entity_id as entity_id,
					 event_timestamp::date as entity_date,
					 'Paid' as kpi_type
					 from stg_kafka_events_full.stream_internal_billing_payments
					 where event_name = 'paid'
					 and entity_type = 'order')
					 		,delivered_stg as (
					select distinct order_number as entity_id,
					event_timestamp::date as entity_date,
					'Delivered' as kpi_type,
					rank() over (partition by order_number order by event_timestamp DESC) as ship_rank
					from stg_kafka_events_full.stream_shipping_shipment_change
					where order_mode = 'MIX'
					and event_name = 'delivered'
					 		)
					 	,delivered as (
					 	select
						 	entity_id,
						 	entity_date,
						 	kpi_type
						 	from
						delivered_stg
						where ship_rank = 1
					 	)
					select *
					from traffic
					union all
					select *
					from add_to_cart
					union all
					select *
					from order_submitted
					union all
					select *
					from order_approved
					union all
					select *
					from paid_orders
					union all
					select *
					from delivered;





--Table for Mix order from submission to delivery
drop table if exists ods_production.mix_order_submitted;

			create table ods_production.mix_order_submitted as
with order_submitted as (
					select order_number as entity_id,
					 cast(timestamp 'epoch' + (published_at::bigint/1000) * interval '1 second'as date) as submitted_date,
					 user_id as customer_id,
					 JSON_EXTRACT_PATH_text(json_extract_array_element_text(line_items,0),'variants') as order_variants,
					True as is_submitted
						from stg_kafka_events_full.stream_internal_order_placed where order_mode = 'MIX'
					)
			,order_approved as (
					select order_number as entity_id,
					event_timestamp::date as decision_date,
					case when decision = 'decline' then 'DECLINED' else 'APPROVED' end as final_decision,
					decision_message
					from stg_kafka_events_full.stream_internal_risk_order_decisions_v3
					)
		,billing_payments as (
					select *, rank() over (partition by entity_id order by event_timestamp DESC) as order_rank from
					 stg_kafka_events_full.stream_internal_billing_payments
					 where entity_type = 'order'
					 and event_name != 'refund')
		,paid_orders as (
					 select entity_id as entity_id,
					 case when event_name = 'paid' then 'PAID'
					  when event_name = 'failed' then 'FAILED PAYMENT' end as paid_status,
					  case when event_name = 'paid' then event_timestamp end as paid_date,
					  case when event_name = 'failed' then event_timestamp end as failed_date
					 from billing_payments
					 where order_rank = 1)
					,ship as (
			select *,rank() over (partition by order_number,event_name order by event_timestamp DESC) as shipment_rank
		from stg_kafka_events_full.stream_shipping_shipment_change
					where order_mode = 'MIX')
			,committed_length as (
					select distinct
			  order_number,
		  committed_length
		  from stg_kafka_events_full.stream_customers_contracts)
	  , ship_create as
		  (select
			  order_number,
			  event_timestamp::timestamp as shipping_label_created_at
			  from ship where event_name = 'created'
			  and shipment_rank = 1
		  )
		,shipped as (
		  select order_number,
		    event_timestamp::timestamp as shipped_at
			  from ship where event_name = 'shipped'
			  and shipment_rank = 1
		  )
	   ,delivered as (
		  select order_number,
		    event_timestamp::timestamp as delivered_at
			  from ship where event_name = 'delivered'
			  and shipment_rank = 1
		  )
		  ,ship_final as (
		  select c.order_number,
		  c.shipping_label_created_at,
		  s.shipped_at,
		  d.delivered_at
		  from ship_create c
		  left join shipped s on s.order_number = c.order_number
		  left join delivered d on s.order_number = d.order_number)
				  select os.entity_id as order_id,
				  Coalesce(po.paid_status,oa.final_decision,case when is_submitted then 'SUBMITTED' end) as order_status,
				  os.submitted_date,
				  os.customer_id,
				  os.order_variants,
				  os.is_submitted,
				  oa.decision_date,
				  oa.final_decision,
				  oa.decision_message,
				  po.paid_status,
				  po.paid_date,
				  po.failed_date,
				  cl.committed_length,
				  case when sf.delivered_at is not null then 'DELIVERED'
				  when sf.shipped_at is not null then 'SHIPPED'
				  when sf.shipping_label_created_at is not null then 'READY TO SHIP'
				  else order_status end as fulfillment_status,
				  sf.shipping_label_created_at,
				  sf.shipped_at,
				  sf.delivered_at
				  from order_submitted os
				  left join order_approved oa on os.entity_id = oa.entity_id
				  left join paid_orders po on os.entity_id = po.entity_id
				  left join committed_length cl on os.entity_id = cl.order_number
		          left join ship_final sf on os.entity_id = sf.order_number;

--Table for Order from Cart to Delivery

			drop table if exists ods_production.mix_order;

			create table ods_production.mix_order as
			 with add_to_cart_all as (
					select
							*,
					case when collector_tstamp > '2020-10-22'
					then json_extract_path_text(se_property,'orderID')
					else json_extract_path_text(se_property,'orderId') end  as order_id,
					case when se_property is not null and is_valid_json(se_property) then json_extract_path_text(se_property,'productData', 'interacting_with_mix') end as is_mix
				 	from scratch.se_events
					where se_action = 'addToCart'
					and is_mix = 'true'
			 		)
			,add_to_cart as (
					select distinct order_id as order_id,
					platform,
					max(collector_tstamp::date) over (partition by order_id order by collector_tstamp rows between unbounded preceding and unbounded following) as add_to_cart_date,
					True as is_cart
					from add_to_cart_all)
				  select ac.*,
				  Coalesce(os.paid_status,os.final_decision,case when os.is_submitted then 'SUBMITTED' end, case when is_cart then 'CART' end) as order_status,
				  	  os.submitted_date,
				  os.customer_id,
				  os.order_variants,
				  os.is_submitted,
				  os.decision_date,
				  os.final_decision,
				  os.decision_message,
				  os.paid_status,
				  os.paid_date,
				  os.failed_date,
				  os.committed_length,
				  case when os.delivered_at is not null then 'DELIVERED'
				  when os.shipped_at is not null then 'SHIPPED'
				  when os.shipping_label_created_at is not null then 'READY TO SHIP'
				  else order_status end as fulfillment_status,
				  os.shipping_label_created_at,
				  os.shipped_at,
				  os.delivered_at
				  from add_to_cart ac
				  left join ods_production.mix_order_submitted os on os.order_id = ac.order_id;

GRANT SELECT ON ods_production.mix_reporting TO tableau;
GRANT SELECT ON ods_production.mix_order TO tableau;
GRANT SELECT ON ods_production.mix_order_submitted TO tableau;
