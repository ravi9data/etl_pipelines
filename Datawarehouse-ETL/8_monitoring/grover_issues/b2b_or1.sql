drop table if exists monitoring.b2b_or1;
create table monitoring.b2b_or1 as
with 
contract_states as (
	select 
		cast(event_timestamp as datetime) event_timestamp  ,
		JSON_EXTRACT_PATH_text(payload,'id') contract_id,
		event_name,
		JSON_EXTRACT_PATH_text(payload,'state') state,
		row_number() over (partition by contract_id order by event_timestamp desc, event_name desc) as idx
	from stg_kafka_events_full.stream_customers_contracts_v2
),
latest_contract_state as (
	select 
		event_timestamp ,
		contract_id,
		case when state = 'ended' 
  			 then 'cancelled' 
  			 else state end as state
	from contract_states
	where idx = 1
),
wemalo_raw as (
	select distinct
		asset_id ,
		seriennummer ,
		stellplatz ,
		coalesce (l.second_level, l.first_level) pos_level,
		lag(pos_level) over (partition by asset_id order by reporting_date asc) prev_pos_level,
		--row_number () over (partition by asset_id order by reporting_date desc) idx,
		reporting_date ,
		previous_reporting_date 
	from dwh.wemalo_sf_reconciliation d
	left join stg_external_apis.wemalo_position_labels l
	on d.stellplatz = l."position" 
	where nullif(stellplatz, '') is not null
 ),
wemalo_report as (
	select  
		asset_id ,
		seriennummer ,
		stellplatz ,
		pos_level ,
		prev_pos_level,
		row_number () over (partition by asset_id order by reporting_date desc) idx,
		reporting_date ,	 
		--exclude both saturday and sunday
		datediff('day', previous_reporting_date::timestamp without time zone, reporting_date::timestamp without time zone) - 
		datediff('week', previous_reporting_date::timestamp without time zone, reporting_date::timestamp without time zone) * 2 as prev_report_diff,
		case when prev_report_diff > 5
			 	  or prev_report_diff is null
			 then true 
			 else false end is_return,
		case when is_return
			 then row_number() over 
			      (partition by asset_id, is_return
							order by reporting_date desc) end idx_return,
		case when --asset may be back direct into a stock position
			      --and previous position may be a stock position as well (before shipping)
			      (is_return 
				   and pos_level not in ('Return', 'Direct Return')
				   and prev_pos_level not in ('Return', 'Direct Return'))
				  or 
				  (pos_level not in ('Return', 'Direct Return')
				  and (prev_pos_level in ('Return', 'Direct Return') or prev_pos_level is null))
				  or 
				  (pos_level in ('Return', 'Direct Return')
				  and (prev_pos_level not in ('Return', 'Direct Return') or prev_pos_level is null))
			 then true 
			 else false end is_stock_change,
		case when is_stock_change
			 then row_number() over 
			      (partition by asset_id, is_stock_change
							order by reporting_date desc) end idx_last
	from wemalo_raw
),
last_wh_positions as (
	select 
		asset_id,
		max(case when idx = 1 then stellplatz end) last_position,
		max(case when idx = 1 then reporting_date end) last_position_date,
		max(case when idx_last = 1 then reporting_date end) first_date_in_last_level,
		max(case when idx_return = 1 then stellplatz end) scan_position,
		max(case when idx_return = 1 then reporting_date end) scan_position_date
	from wemalo_report
	group by 1
)
, main_script as (
select 
	coalesce (a.order_id, s.order_id ) order_id, --one of allocation does not have order_id
	a.subscription_id ,
	a.allocation_id ,
	case when a.allocation_sf_id is null then 'New Infra'
		 else 'Legacy' end as infra,
    c.company_name,
	a.serial_number ,
	a.asset_id ,
	round(t.residual_value_market_price, 0) as residual_value_market_price,
	s.subscription_value,
	--s.status , 
	upper(coalesce (lcs.state, s.status)) as status,
	coalesce (lcs.event_timestamp, s.updated_date ) as status_date,
	case when a.is_last_allocation_per_asset is null then
			  --we only calculate this if allocation is not cancelled or not fixed
			  --however, since we are not excluding these, we still need to confirm
			  --if asset is with different customer for now
			  case when t.customer_id <> s.customer_id then false else true end
		 else a.is_last_allocation_per_asset end as is_last_allocation_per_asset_n,
	t.asset_status_original,
	case when not is_last_allocation_per_asset_n
			  or t.asset_status_original not in ('RETURNED', 'RESERVED', 'ON LOAN') 
		 then true else false end as has_put_in_stock, --check for in stock and other sperlagger
	a.allocation_status_original ,
	a.delivered_at ,
	a.return_shipment_at ,
	a.return_delivery_date ,
	a.refurbishment_start_at, 
	w.last_position,
	w.last_position_date,
	case when w.last_position_date > a.delivered_at then l.first_level else null end as first_level,
	case when w.last_position_date > a.delivered_at then l.second_level else null end as second_level,
	w.first_date_in_last_level,
	w.scan_position,
	w.scan_position_date,
	case when w.scan_position_date > a.delivered_at --if it is scanned by wh after return
		 then datediff('day', a.delivered_at::timestamp without time zone, w.scan_position_date::timestamp without time zone ) 
		 end as days_delivered_to_scanned,
	case when w.scan_position_date > a.return_delivery_date --if it is scanned by wh after return
		 then datediff('day', a.return_delivery_date::timestamp without time zone, w.scan_position_date::timestamp without time zone ) 
		 end as days_returned_to_scanned ,
	datediff('day', w.scan_position_date::timestamp without time zone,   w.last_position_date::timestamp without time zone ) as days_scanned_to_last,
	case when coalesce (a.return_delivery_date, a.return_shipment_at) is null 
		 then false else true end as with_label,
	case when greatest(w.last_position_date, w.scan_position_date) > a.delivered_at
			  and l.first_level not in ('Direct Return', 'Return')
		 then true else false end as is_refurbished,
	case when greatest(w.last_position_date, w.scan_position_date) > a.delivered_at
			  and l.first_level in ('Direct Return', 'Return')
		 then true else false end as is_scanned,	 
	case when with_label 
	     	  and not has_put_in_stock
	     	  and is_refurbished
	     	then 'UC3' 
		 when with_label 
	     	  and not has_put_in_stock
	     	  and not is_refurbished
	     	  and is_scanned
	     	then 'UC2'
		 when with_label 
	     	  and not has_put_in_stock
	     	  and not is_refurbished
	     	  and not is_scanned
	     	then 'UC1'
		 when not with_label 
			  and not has_put_in_stock
			  and is_refurbished
	     	then 'UC5' 
	     when not with_label 
	     	  and not has_put_in_stock
	     	  and not is_refurbished
	     	  and is_scanned
	     	then 'UC4' 
	     else null end use_case_code,
	case when use_case_code in ('UC1')
		 	then datediff('day', greatest(a.return_delivery_date::timestamp without time zone, a.return_shipment_at::timestamp without time zone), current_date )
		 when use_case_code in ('UC2', 'UC4')
		 	then datediff('day', w.scan_position_date::timestamp without time zone, current_date)
		 when use_case_code in ('UC3', 'UC5')
		 	then datediff('day', w.first_date_in_last_level::timestamp without time zone, current_date)
		 else 0 
		 end as days_impacted
from ods_production.allocation a
left join master.asset t on a.asset_id = t.asset_id 
left join ods_production.subscription s on a.subscription_id = s.subscription_id 
left join ods_production.customer c on s.customer_id = c.customer_id
left join latest_contract_state lcs on a.subscription_id = lcs.contract_id
left join last_wh_positions w on a.asset_id = w.asset_id
left join stg_external_apis.wemalo_position_labels l on w.last_position = l."position" 
where c.customer_type = 'business_customer'
	and a.subscription_id is not null
	--not sure?
	and a.delivered_at is not null 
	and s.country_name <> 'United States'
	--exclude today's return, since we may not have most current data
	and (a.return_delivery_date is null or
		 a.refurbishment_start_at is null or 
		 date_trunc('day', greatest (a.return_delivery_date, a.refurbishment_start_at)) <> current_date)
	--exclude not returned or in transit, confirm that asset not reported yet
	and not (a.return_delivery_date is null
			 and greatest(w.last_position_date, w.scan_position_date) < a.delivered_at)	 
	and t.asset_status_original 
		 not in ('IN DEBT COLLECTION',
				 'SOLD',
			     'LOST',
				 'DELETED',
				 'WRITTEN OFF DC',
				 'WRITTEN OFF OPS')
)
-------
-------
select 
	order_id,
	subscription_id,
	allocation_id,
	infra,
    company_name,
	serial_number,
	asset_id,
	residual_value_market_price,
	subscription_value,
	status,
	--status_date,
	--is_last_allocation_per_asset_n,
	asset_status_original,
	--has_put_in_stock,
	allocation_status_original,
	delivered_at,
	return_shipment_at,
	return_delivery_date,
	--with_label,
	--refurbishment_start_at,
	scan_position_date,
	scan_position,
	first_date_in_last_level,
	last_position_date,
	last_position,
	first_level,
    second_level,
	days_delivered_to_scanned,
	days_returned_to_scanned ,
	days_scanned_to_last,
	use_case_code,
	days_impacted
from main_script
where (return_delivery_date is null or
		datediff('day', 
				 greatest (return_delivery_date, return_shipment_at, scan_position_date)::timestamp without time zone,
				 current_date) > 1 ) 
	--include all returns, to be used in redash - exclude returns without use case code in tableau
;


--append daily summary
delete from monitoring.b2b_or1_summary
where reporting_date = current_date::date;

insert into monitoring.b2b_or1_summary  
select 
    current_date::date as reporting_date,
    use_case_code ,
    sum(residual_value_market_price) total_asset_valuation,
    sum(subscription_value) total_subscription_value ,
    count(asset_id) total_assets_impacted,
    count(distinct order_id) total_orders_impacted
from monitoring.b2b_or1
where use_case_code is not null
group by 1,2;

GRANT SELECT ON monitoring.b2b_or1 TO tableau;
GRANT SELECT ON monitoring.or1 TO tableau;
