drop table if exists ods_b2b.offers;
create table ods_b2b.offers as 
with offers_accepted as (
		select 
		id, 
		name, 
		created_at, 
		updated_at, 
		order_id,
		customer_id, 
		owner_id, 
		expires_in, 
		status as offer_status
		from stg_curated.b2b_eu_offer_accepted_v1 )
		, offers_rejected as (
		select 
		id,
		name, 
		created_at,
		updated_at, 
		order_id,
		customer_id, 
		owner_id, 
		expires_in, 
		status as offer_status
		from stg_curated.b2b_eu_offer_rejected_v1)
		, offers_activated as (
		select 
		id,
		name, 
		created_at,
		updated_at, 
		order_id,
		customer_id, 
		owner_id, 
		expires_in, 
		status as offer_status
		from stg_curated.b2b_eu_offer_activated_v1)
		, union_ as (
		select * from offers_accepted
		union all
		select * from offers_rejected
		union all
		select * from offers_activated)
		, idx as
		(
		select distinct id, offer_status,
		row_number() over (partition by u.id order by updated_at desc) as idx
		from union_ u
		)
--		select * from idx 
		, dates as (
		select 
			u.id,
			u.name as offer_name, 
			u.order_id, 
			u.customer_id,
			u.owner_id, 
			case when i.idx = 1 then i.offer_status else null end as offer_status,
			min(u.created_at) as created_at, 
			min(u.expires_in) as expires_in, 
			min(case when u.offer_status ='ACTIVATED' then updated_at end) as activated_at, 
			min(case when u.offer_status ='ACCEPTED' then updated_at end) as accepted_at, 
			min(case when u.offer_status ='REJECTED' then updated_at end) as rejected_at, 
			max(u.updated_at) as updated_at
		from union_ u
		left join idx i
			on i.id = u.id
			and i.idx =1
		group by 1, 2, 3, 4, 5, 6)
		select 
			d.id, 
			d.offer_name,
			d.order_id, 
			d.customer_id, 
			d.owner_id, 
			case 
				when accepted_at is null and rejected_at is null and d.expires_in < current_date then 'EXPIRED'
			else d.offer_status end as offer_status, 
			d.created_at, 
			d.updated_at,
			d.expires_in,
			d.activated_at,
			d.accepted_at,
			d.rejected_at,
			r.rejected_by_id,
			case 
				when r.rejected_by_id = d.customer_id then 'Rejected by Customer'
				when r.rejected_by_id = d.owner_id then 'Rejected by Owner'
				when r.rejected_by_id is null then null
				else 'Check'
			end as rejected_label, 
			--order table
            o.store_country, 
			o.status, 
			o.is_pay_by_invoice,
			o.paid_date
		from dates d
			left join stg_curated.b2b_eu_offer_rejected_v1 r 
				on r.id = d.id
			left join ods_production.order o
				on o.order_id = d.order_id;

GRANT SELECT ON ods_b2b.offers TO tableau;
