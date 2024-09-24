drop table if exists dm_recommerce.repair_invoices;
create table dm_recommerce.repair_invoices as

with union_all as (
	select 
		'tmx' as repair_partner,
		tmx_claim_nr::text as kva_id,
		upper(imei_serialnumber_in) as serial_number_before,
		case when imei_serialnumber_out is null 
				  or imei_serialnumber_out in ('#NV')
			 then serial_number_before 
			 else upper(imei_serialnumber_out)
			 end as serial_number,
		case when REGEXP_COUNT(serial_number, '^[0-9]+$') = 1 
				  and len(serial_number) = 15 
			 then serial_number end as imei,
		null as repaired_asset_name,	 
		cost_estimate_date::date as cost_estimate_date,
		inbound_date::date as inbound_date,
		outbound_date::date as outbound_date, 
		repair_description as repair_description,
		repair_price as repair_price
	from stg_external_apis_dl.repair_partner_tmr
	union
	select 
		'letmerepair' as repair_partner,
		opalorder::text as kva_id,
		null as serial_number_before,
		upper(serial) as serial_number,
		nullif(imei::text, '(Leer)') as imei,
		productdescription as repaired_asset_name,
		null as cost_estimate_date,
		inbound::date as inbound_date,
		to_date(shippeddate, 'dd/mm/yyyy') as outbound_date,
		null as repair_description,
		gesamt_brutto as repair_price
	from stg_external_apis_dl.repair_partner_lmr 
	union
	select 
		'mttc' as repair_partner,
		internal_id as kva_id,
		upper(imei_serialnumber_in) as serial_number_before,
		case when imei_serialnumber_out is null 
			 then serial_number_before
			 else upper(imei_serialnumber_out)
			 end as serial_number,
		null as imei,
		null as repaired_asset_name,
		to_date(nullif(cost_estimate_date::text, '#NV'),
				  'dd/mm/yyyy') as cost_estimate_date,
		to_date(to_char(inbound_date, 'dd/mm/yy'),
				  'dd/mm/yy') as inbound_date,
		to_date(to_char(outbound_date, 'dd/mm/yy'),
				  'dd/mm/yy') as outbound_date,
	    repair_description as repair_description,
	    repair_price
	from stg_external_apis_dl.repair_partner_mttc 
	union
	select 
		'finito' as repair_partner,
		internal_id::text as kva_id,
		null as serial_number_before,
		upper(imei_sn) as serial_number,
		null as imei,
		"type" as repaired_asset_name,
		null as cost_estimate_date,
		to_date(inbound_date , 'dd/mm/yyyy') as inbound_date,
		to_date(outbound_date , 'dd/mm/yyyy') as outbound_date,
	    replace(trim(repair_description), CHR(10), ' ') as repair_description,
	    repair_price
	from stg_external_apis_dl.repair_partner_finitoo 
	union
	select 
		'sertec' as repair_partner,
		caseid as kva_id,
		null as serial_number_before,
		upper(serialnumber) as serial_number,
		null as imei,
		productbrand || ' ' || fgpartdescription as repaired_asset_name,
		null as cost_estimate_date,
		to_date(receivedate , 'dd/mm/yyyy') as inbound_date,
		to_date(closedate , 'dd/mm/yyyy') as outbound_date,
		internaldiagnose as repair_description,
		replace(
		replace(kosten_gesamt, '€', ''), 
				',', '.')::float as repair_price
	from stg_external_apis_dl.repair_partner_sertec 
	union
	select 
		'rss' as repair_partner,
		internal_id as kva_id,
		upper(imei_serialnumber_in) as serial_number_before,
		upper(imei_serialnumber_out) as serial_number,
		null as imei,
		null as repaired_asset_name,
		cost_estimate_date::date as cost_estimate_date,
		inbound_date::date as inbound_date,
		outbound_date::date as outbound_date,
		repair_description ,
		repair_price 
	from stg_external_apis_dl.repair_partner_rssrepair
	),
fix_tmx_apple_sn as (
	select 
		u.kva_id,
		case when u.repair_partner = 'tmx'
				  and u.serial_number <> r.serial_number 
				  and r.serial_number is not null
			 then r.serial_number 
			 else u.serial_number end as serial_number, 
		u.imei,
		u.serial_number_before,
		u.repaired_asset_name,
		u.cost_estimate_date,
		u.inbound_date,
		u.outbound_date,
		u.repair_description,
		u.repair_price,
		u.repair_partner
	from union_all u 
	left join dm_recommerce.repair_cost_estimates r 
	on u.kva_id = r.kva_id
),
sn_swap as (
	select 
		u.kva_id,
		case when c.new_serial is not null then c.new_serial 
			 when sns.new_serial is not null then sns.new_serial 
			 else coalesce (u.imei::text, upper(u.serial_number)) end as serial_number ,
		u.imei,
  		case when c.new_serial is not null then 'Corrected'
  			 when sns.new_serial is not null then 'Swapped'
  			 else 'Original' end as swap_status,
		u.serial_number_before,
		u.repaired_asset_name,
		u.cost_estimate_date,
		u.inbound_date,
		u.outbound_date,
		u.repair_description,
		u.repair_price,
		u.repair_partner
	from fix_tmx_apple_sn u 
	left join dm_recommerce.corrected_sn c on c.old_serial = coalesce (u.imei::text, upper(u.serial_number)) --imported from gsheet
	left join dm_recommerce.serial_number_swap sns on sns.old_serial = coalesce (u.imei::text, upper(u.serial_number)) --imported from gsheet
	), 
enrich_sf as (
	select 
		u.kva_id,
		u.serial_number,
		u.imei,
		u.serial_number_before,
		u.swap_status,
		u.repaired_asset_name,
		a.asset_name ,
		a.category_name ,
		a.subcategory_name ,
		a.brand ,
		a.product_sku ,
		a.variant_sku ,
		u.cost_estimate_date,
		u.inbound_date,
		u.outbound_date,
		u.repair_description,
		u.repair_price::float,
		u.repair_partner
	from sn_swap u 
	left join master.asset a on u.serial_number = a.serial_number 
)
select * from enrich_sf;

GRANT SELECT ON dm_recommerce.repair_invoices TO redash_pricing;

GRANT SELECT ON dm_recommerce.repair_invoices TO tableau;
GRANT SELECT ON dm_recommerce.repair_invoices TO GROUP mckinsey;
