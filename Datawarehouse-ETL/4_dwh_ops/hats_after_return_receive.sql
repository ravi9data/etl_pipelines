drop table if exists hats_enriched;
create temp table hats_enriched as 
with rd_hats as (
	select 
		poll_slug , 
		vote_slug , 
		entity_id ,
		"comment" ,
		created_at 
	from staging.spectrum_polls_votes 
	where poll_slug in ('likelihood-recommend',
						'last-order-delivery-experience',
						'delivery-xp-reasons', 
						'which-social-media-channel', 
						'where-found-about',
						'after-return-likelihood-recommend',
						'after-return-rental-experience',
						'on-last-return-experience',
						'return-reasons',
						'after-return-rental-xp-reasons',
						'return-xp-reasons')
	and created_at >= dateadd('month', -12, date_trunc('month', current_date))
),
get_carrier_country as (
	select 
		order_id as entity_id ,
		warehouse,
		shipping_country,
		carrier,
		coalesce(shipment_service, carrier) as shipment_service,
		row_number() over (partition by order_id order by coalesce (shipment_at, allocated_at) desc) rn
	from ods_operations.allocation_shipment 
	where order_id in 
		(select entity_id 
		from rd_hats 
		where poll_slug in ('likelihood-recommend', 'delivery-xp-reasons'))
	union all 
	select 
		return_shipment_uid as entity_id , 
		warehouse,
		shipping_country ,
		return_carrier as carrier ,
		return_carrier as shipment_service , --we do not have service info
		row_number() over (partition by return_shipment_uid order by coalesce (return_shipment_at, allocated_at) desc) rn
	from ods_operations.allocation_shipment 
	where return_shipment_uid in 
		(select entity_id 
		  from rd_hats 
		 where poll_slug in ('after-return-likelihood-recommend', 'on-last-return-experience'))	 
)
select 
	case when r.poll_slug in ('after-return-likelihood-recommend',
							  'after-return-rental-experience',
							  'on-last-return-experience',
							  'return-reasons',
							  'after-return-rental-xp-reasons',
							  'return-xp-reasons')
			 then 'After Return'
			 else 'After Receive'
		end as poll_bucket,
	r.poll_slug ,
	r.vote_slug ,
	r.entity_id ,
	r."comment",
	created_at :: timestamp,
	c.warehouse,
	c.shipping_country,
	c.carrier,
	c.shipment_service
from rd_hats r 
left join get_carrier_country c 
on r.entity_id = c.entity_id
where c.rn = 1;


drop table if exists dm_operations.hats_after_return_receive; 
create table dm_operations.hats_after_return_receive as
select 
	poll_bucket ,
	poll_slug ,
	vote_slug ,
	date_trunc('week', created_at) fact_date,
	warehouse,
	shipping_country ,
	carrier ,
	shipment_service ,
	count(1) total_votes
from hats_enriched
group by 1,2,3,4,5,6,7,8;

GRANT SELECT ON dm_operations.hats_after_return_receive TO tableau;

--comments and revocation parts will be added later.