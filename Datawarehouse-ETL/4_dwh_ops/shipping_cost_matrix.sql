drop table if exists new_costs;
create temp table new_costs as
with 
_sc as (
	--to make the table name shorter in next query
	select * from staging_airbyte_bi.k8s_shipping_costs_sheet1
), 
rawd (product_sku , carrier, origin, destination, raw_price_outbound, raw_price_inbound) as (
	--DHL FROM/TO GERMANY
	select product_sku, 'DHL', 'Germany' , 'Germany', dhl_de_final, return_dhl_de_final from _sc union
	select product_sku, 'DHL', 'Germany' , 'Austria', dhl_at_final, return_dhl_at_final from _sc union
	select product_sku, 'DHL', 'Germany' , 'Netherlands', dhl_nl_final, return_dhl_nl_final from _sc union
	select product_sku, 'DHL', 'Germany' , 'Spain', dhl_es_final, return_dhl_es_final from _sc union
	--DHL FROM/TO NETHERLANDS
	select product_sku, 'DHL', 'Netherlands' , 'Germany', dhl_de_final, return_dhl_de_final from _sc union
	select product_sku, 'DHL', 'Netherlands' , 'Austria', dhl_at_final, return_dhl_at_final from _sc union
	select product_sku, 'DHL', 'Netherlands' , 'Netherlands', dhl_nl_final, return_dhl_nl_final from _sc union
	select product_sku, 'DHL', 'Netherlands' , 'Spain', dhl_es_final, return_dhl_es_final from _sc union
	--UPS FROM/TO GERMANY
	select product_sku, 'UPS', 'Germany' , 'Germany', ups_de_de_final, return_ups_de_de_final from _sc union
	select product_sku, 'UPS', 'Germany' , 'Austria', ups_de_at_final, return_ups_de_at_final from _sc union
	select product_sku, 'UPS', 'Germany' , 'Netherlands', ups_de_nl_final, return_ups_de_nl_final from _sc union
	select product_sku, 'UPS', 'Germany' , 'Spain', ups_de_es_final, return_ups_de_es_final from _sc union
	--UPS FROM/TO NETHERLANDS
	select product_sku, 'UPS', 'Netherlands' , 'Germany', ups_nl_de_final, return_ups_nl_de_final from _sc union
	select product_sku, 'UPS', 'Netherlands' , 'Austria', ups_nl_at_final, return_ups_nl_at_final from _sc union
	select product_sku, 'UPS', 'Netherlands' , 'Netherlands', ups_nl_nl_final, return_ups_nl_nl_final from _sc union
	select product_sku, 'UPS', 'Netherlands' , 'Spain', ups_nl_es_final, return_ups_nl_es_final from _sc union
	--HERMES FROM/TO GERMANY
	select product_sku, 'HERMES', 'Germany' , 'Germany', hermes_de_final, return_hermes_de_final from _sc union
	select product_sku, 'HERMES', 'Germany' , 'Austria', hermes_at_final, return_hermes_at_final from _sc union
	select product_sku, 'HERMES', 'Germany' , 'Netherlands', hermes_nl_final, return_hermes_nl_final from _sc union
	select product_sku, 'HERMES', 'Germany' , 'Spain', hermes_es_final, return_hermes_es_final from _sc union
	--HERMES FROM/TO GERMANY
	select product_sku, 'HERMES' ,'Netherlands' , 'Germany', hermes_de_final, return_hermes_de_final from _sc union
	select product_sku, 'HERMES' ,'Netherlands' , 'Austria', hermes_at_final, return_hermes_at_final from _sc union
	select product_sku, 'HERMES' ,'Netherlands' , 'Netherlands', hermes_nl_final, return_hermes_nl_final from _sc union
	select product_sku, 'HERMES' ,'Netherlands' , 'Spain', hermes_es_final, return_hermes_es_final from _sc 
)
select 
	product_sku ,		 
	carrier,
	origin,
	destination ,
	replace( raw_price_outbound , '#N/A', 0) ::float price_outbound ,
	replace( raw_price_inbound , '#N/A', 0) ::float price_inbound ,
	min(nullif (price_outbound, 0)) over (partition by product_sku, origin, destination) best_outbound ,
	min(nullif (price_inbound,  0)) over (partition by product_sku, origin, destination) best_inbound 
from rawd ;


insert into dm_operations.shipping_costs
with new_prices as (
	select product_sku, carrier, origin, destination, price_outbound, price_inbound, best_outbound, best_inbound
	from new_costs
	minus
	select product_sku, carrier, origin, destination, price_outbound, price_inbound, best_outbound, best_inbound
	from dm_operations.shipping_costs
)
select n.product_sku, n.carrier, n.origin, n.destination, n.price_outbound, n.price_inbound, n.best_outbound, n.best_inbound
	, case when (select count(1) 
				   from dm_operations.shipping_costs s 
				  where s.product_sku = n.product_sku
				  	and s.carrier = n.carrier
					and s.origin = s.origin
					and s.destination = n.destination) > 0 
			then current_date
			else '2000-01-01'::timestamp 
			end published_date
from new_prices n;



drop table if exists dm_operations.shipping_costs_comparison;
create table dm_operations.shipping_costs_comparison as
with outbounds as (
	select 
		'outbound' as shipment_type,
		allocation_id ,
		customer_type ,
		split_part(variant_sku , 'V' , 1) product_sku,
		shipment_at,
		carrier ,
		shipment_service,
		case when warehouse in ('ingram_micro_eu_flensburg', 'synerlogis_de')  --do not use sender_country , because earlier, we specifically mention altenholz for failed deliveries and returns
			 then 'Germany'
			 else 'Netherlands'
		end sender_country , --we could move this logic to all.ship. table later.
		receiver_country 
	from ods_operations.allocation_shipment 
	where shipment_at > current_date - 180
	and carrier in ('UPS', 'DHL', 'HERMES')
	and warehouse in ('ingram_micro_eu_flensburg', 'ups_softeon_eu_nlrng', 'synerlogis_de')
	and receiver_country in ('Germany', 'Netherlands', 'Spain', 'Austria')
),
inbounds as (
	select 
		'inbound' as shipment_type,
		allocation_id ,
		customer_type ,
		split_part(variant_sku , 'V' , 1) product_sku ,
		return_shipment_at as shipment_at ,
		return_carrier as carrier ,
		return_carrier as shipment_service ,
		--shipping costs listed in reverse order for returns
		return_receiver_country as sender_country , 
		return_sender_country as receiver_country 
	from ods_operations.allocation_shipment 
	where return_shipment_at > current_date - 180
	and return_carrier in ('UPS', 'DHL', 'HERMES')
	and return_receiver_country  in ('Germany', 'Netherlands')
	and return_sender_country in ('Germany', 'Netherlands', 'Spain', 'Austria')
),
all_shipments as (
	select shipment_type, allocation_id , customer_type , product_sku , shipment_at , carrier , shipment_service, sender_country , receiver_country from outbounds 
	union
	select shipment_type, allocation_id , customer_type , product_sku , shipment_at , carrier , shipment_service, sender_country , receiver_country from inbounds
),
get_costs as (
	select 
		a.shipment_type, 
		date_trunc('week', a.shipment_at) fact_date , 
		a.allocation_id , 
		a.customer_type,
		a.product_sku , 
		a.carrier , 
		a.shipment_service,
		a.sender_country , 
		a.receiver_country,
		case when a.shipment_type = 'outbound' then c.price_outbound else c.price_inbound end paid_price,
		case when a.shipment_type = 'outbound' then c.best_outbound else c.best_inbound end best_price,
		row_number() over (partition by a.allocation_id, a.shipment_type order by c.published_date desc) as rn
	from all_shipments a 
	left join dm_operations.shipping_costs c 
	on a.carrier = c.carrier
	and a.sender_country = c.origin
	and a.receiver_country = c.destination
	and a.product_sku = c.product_sku
	where a.shipment_at > c.published_date
),
dims_pre as (
	select
		product_sku,
		final_height * final_width * final_length / 1000000 as _volume_bucket,
		final_weight as _weight_bucket,
		row_number() over (partition by product_sku order by is_ups_dim_available desc) rn 
	from dm_operations.dimensions
),
dims as (
	select product_sku, _volume_bucket, _weight_bucket from dims_pre where rn = 1
)
select 
	gc.shipment_type ,
	gc.fact_date ,
	gc.product_sku ,
	gc.customer_type,
	case when d._volume_bucket < 0.1 then '< 0.1 m3'
		 when d._volume_bucket < 0.5 then '0.1-0.5 m3'
		 when d._volume_bucket < 1   then '0.5-1 m3'
		 when d._volume_bucket < 2   then '1-2 m3'
		 when d._volume_bucket < 3   then '2-3 m3'
		 else '> 3 m3'
	end as volume_bucket,
	case when d._weight_bucket < 1 then '< 1 kg'
		 when d._weight_bucket < 3 then '1-3 kg'
		 when d._weight_bucket < 5   then '3-5 kg'
		 when d._weight_bucket < 10   then '5-10 kg'
		 when d._weight_bucket < 30   then '10-30 kg'
		 else '> 30 kg'
	end as weight_bucket,
	p.product_name ,
	p.category_name ,
	gc.carrier,
	gc.shipment_service,
	gc.sender_country,
	gc.receiver_country,
	count(case when gc.rn = 1 then gc.allocation_id end) total_allocations,
	count(case when gc.rn = 1 and paid_price = best_price then 1 end) total_allocations_with_best_price,
	sum(case when gc.rn = 1 then paid_price end) total_price_paid,
	sum(case when gc.rn = 1 then best_price end) total_best_price
from get_costs gc 
left join ods_production.product p 
on gc.product_sku = p.product_sku 
left join dims d
on gc.product_sku = d.product_sku
group by 1,2,3,4,5,6,7,8,9,10,11,12
;

GRANT SELECT ON dm_operations.shipping_costs_comparison TO tableau;
GRANT SELECT ON staging_airbyte_bi.k8s_shipping_costs_sheet1 TO hightouch_pricing, redash_pricing;
GRANT SELECT ON staging_airbyte_bi.k8s_shipping_costs_sheet1 TO GROUP mckinsey;
GRANT SELECT ON dm_operations.shipping_costs_comparison TO GROUP recommerce_data;
