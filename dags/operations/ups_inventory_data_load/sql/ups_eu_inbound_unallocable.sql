drop table if exists dm_operations.ups_eu_inbound_unallocable;

create table dm_operations.ups_eu_inbound_unallocable as
with
recon as (
	select
		sku,
		description,
		count(case when unallocable_bucket = 'UNALLOCABLE' then 1 end) unallocable_in_sf,
		count(case when unallocable_bucket = 'OTHER STATUS' then 1 end) other_status_in_sf,
		count(case when unallocable_bucket = 'MISMATCH' then 1 end) missing_in_sf,
		count(case when unallocable_bucket = 'ONLY UNALLOCABLE IN SF' then 1 end) only_unallocable_in_sf
	from dm_operations.ups_eu_reconciliation u
	where is_latest_report
	and unallocable_bucket is not null
	group by 1, 2) ,
ups_summary as (
	select
		sku,
		unallocable_kitted_qty,
		unallocable_not_kitted_qty
	from stg_external_apis.ups_nl_ib_unallocable
	where report_date = (select max(report_date) from stg_external_apis.ups_nl_ib_unallocable )
  ),
alljoin as (
	select
		r.sku,
		r.description ,
		r.unallocable_in_sf,
		r.other_status_in_sf,
		r.missing_in_sf,
		r.only_unallocable_in_sf,
		us.unallocable_kitted_qty,
		us.unallocable_not_kitted_qty
	from recon r
	join ups_summary us
	on r.sku = us.sku
),
wemalo_in_stock as (
	select
		wemalo_sku ,
		count(distinct seriennummer) wemalo_stock
	from dwh.wemalo_sf_reconciliation
	where second_level = 'Stock'
		  and reporting_date = (select max(reporting_date) from dwh.wemalo_sf_reconciliation )
	group by 1
),
categories as (
  select distinct variant_sku,
    category_name,
    subcategory_name
   from master.asset
  )
  select
  	a.sku,
    c.category_name,
    c.subcategory_name,
    a.description,
	a.unallocable_in_sf,
	a.other_status_in_sf,
	a.missing_in_sf,
	a.only_unallocable_in_sf,
	a.unallocable_kitted_qty,
	a.unallocable_not_kitted_qty,
	w.wemalo_stock
  from alljoin a
  left join categories c
  on a.sku = c.variant_sku
  left join wemalo_in_stock w
  on a.sku = w.wemalo_sku
  ;


grant all on dm_operations.ups_eu_inbound_unallocable to group bi;
GRANT SELECT ON dm_operations.ups_eu_inbound_unallocable TO tableau;
