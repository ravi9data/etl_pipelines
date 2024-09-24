update  staging.repair_partner_tmx
set "inbound date"=null
where "inbound date" ='-';

update  staging.repair_partner_tmx
set "cost estimate date"=null
where "cost estimate date" ='-';

drop table if exists dm_recommerce.repair_invoices;
create table dm_recommerce.repair_invoices as
with union_all as (
select 	'tmx' as  repair_partner,
		"tmx claim nr."::text as kva_id,
		upper("imei/serialnumber in") as serial_number_before,
		case when "imei/serialnumber out" is null
				  or "imei/serialnumber out" in ('#NV')
			 then serial_number_before
			 else upper("imei/serialnumber out")
			 end as serial_number,
		case when REGEXP_COUNT(serial_number, '^[0-9]+$') = 1
 				  and len(serial_number) = 15
 			 then serial_number end as imei,
 		null as repaired_asset_name,
   case when "cost estimate date"='-' then replace("cost estimate date",'-',null)::date
    	 when  substring("cost estimate date",2,1)='.' or substring("cost estimate date",3,1)='.' then to_date("cost estimate date", 'dd/mm/yyyy')
         else "cost estimate date"::date end as cost_estimate_date,
    case when substring("inbound date",2,1)='.' or substring("inbound date",3,1)='.'
	then to_date("inbound date", 'dd/mm/yyyy') else "inbound date"::date end as inbound_date,
	case when substring("outbound date",2,1)='.' or substring("outbound date",3,1)='.'
	then to_date("outbound date", 'dd/mm/yyyy') else "outbound date"::date end as outbound_date,
 		"repair description" as repair_description,
		replace(replace(trim(replace("repair price",'€', '')),'.',''),',','.')::float as repair_price
	from staging.repair_partner_tmx
union
	select
		'letmerepair' as repair_partner,
		opalorder::text as kva_id,
		null as serial_number_before,
		upper(serial) as serial_number,
		nullif(imei, '(Leer)') as imei,
		productdescription as repaired_asset_name,
		null::Date as cost_estimate_date,
		to_date("inbound",'dd/mm/yyyy') as inbound_date,
		case when substring("shippeddate",2,1)='.' or substring("shippeddate",3,1)='.'
	    then to_date("shippeddate", 'dd/mm/yyyy') else "shippeddate"::date end as outbound_date,
		null as repair_description,
		replace(trim(replace("gesamt brutto",'€', '')),',','.')::float as repair_price
	from staging.repair_partner_lmr
	union
	select
		'reperando' as repair_partner,
		"internal id" as kva_id,
		upper("imei/serialnumber in") as serial_number_before,
		case when "imei/serialnumber out" is null
			 then serial_number_before
			 else upper("imei/serialnumber out")
			 end as serial_number,
		null as imei,
		null as repaired_asset_name,
		null as cost_estimate_date,
		"inbound date"::date as inbound_date,
		"outbound date"::date as outbound_date,
		"repair description" as repair_description,
		replace(trim(replace("repair price",'€', '')),',','.')::float as repair_price
	from staging.repair_partner_reperando rpr
	union
select
		'mttc' as repair_partner,
		"internal id" as kva_id,
		upper("imei/serialnumber in") as serial_number_before,
		case when "imei/serialnumber out" is null
			 then serial_number_before
			 else upper("imei/serialnumber out")
			 end as serial_number,
 		null as imei,
 		null as repaired_asset_name,
 		case when substring("cost estimate date",3,1)='.'  then to_date("cost estimate date",'dd/mm/yyyy')
 			 when substring("cost estimate date",5,1)='-' then to_date("cost estimate date",'yyyy/mm/dd')
			 when substring("cost estimate date",3,1)='-' then to_date("cost estimate date",'dd/mm/yy')
 			 when "cost estimate date"='#NV' then null
 		else "cost estimate date"::date end as cost_estimate_date,
 		inbounddate::date as inbound_date,
 	    outbounddate::date as outbound_date,
 	    "repair description" as repair_description,
 	     replace(trim(replace("repair price",'€', '')),',','.')::float as repair_price
	from staging.repair_partner_mttc
	where outbounddate<>'#NV'
	union
select
		'refixo' as repair_partner,
		"orderid" as kva_id,
		upper("sn-in") as serial_number_before,
		case when "sn-out" is null
			 then serial_number_before
			 else upper("sn-out")
			 end as serial_number,
		null as imei,
		null as repaired_asset_name,
		null as cost_estimate_date,
		to_date("in-date", 'dd/mm/yyyy') as inbound_date,
		to_date("out-date", 'dd/mm/yyyy') as outbound_date,
		null as repair_description,
		replace("re-price-with-tax",',','.')::float as repair_price
	from staging.repair_partner_refixo
	union
select
		'finito' as repair_partner,
		"internal id"::text as kva_id,
		null as serial_number_before,
		upper("imei/sn") as serial_number,
		null as imei,
		"typ" as repaired_asset_name,
		null::date as cost_estimate_date,
		case when substring("inbound date",2,1)='.' or substring("inbound date",3,1)='.'
	    then to_date("inbound date", 'dd/mm/yyyy') else "inbound date"::date end as inbound_date,
		case when substring("outbound date",2,1)='.' or substring("outbound date",3,1)='.'
	    then to_date("outbound date", 'dd/mm/yyyy') else "outbound date"::date end as outbound_date,
	    replace(trim("repair description"), CHR(10), ' ') as repair_description,
	    replace(replace(trim(replace("repair price",'€', '')),'.',''),',','.')::float as repair_price
from staging.repair_partner_finitoo
union
select
'sertec' as repair_partner,
		caseid as kva_id,
		null as serial_number_before,
		upper(serialnumber) as serial_number,
		null as imei,
		productbrand || ' ' || fgpartdescription as repaired_asset_name,
		null::date as cost_estimate_date,
		case when substring(receivedate,3,1)='.'  then to_date(receivedate,'dd/mm/yyyy')
 			 when substring(receivedate,5,1)='-' then to_date(receivedate,'yyyy/mm/dd')
 		else receivedate::date end as inbound_date,
 		case when substring(closedate,3,1)='.'  then to_date(closedate,'dd/mm/yyyy')
 			 when substring(closedate,5,1)='-' then to_date(closedate,'yyyy/mm/dd')
 		else closedate::date end as outbound_date,
		internaldiagnose as repair_description,
		replace(trim(replace("kosten gesamt",'€', '')),',','.')::float as repair_price
from staging.repair_partner_sertec360
union
	select
		'rss' as repair_partner,
		"internal id" as kva_id,
		upper("imei/serialnumber in") as serial_number_before,
		upper("imei/serialnumber out") as serial_number,
		null as imei,
		null as repaired_asset_name,
		"cost estimate date"::date as cost_estimate_date,
		"inbound date"::date as inbound_date,
		"outbound date"::date as outbound_date,
		"repair description" as repair_description ,
		replace(trim(replace("repair price",'€', '')),',','.')::float as repair_price
		from staging.repair_partner_rssrepair
		),
repair_cost_estimates_pre as(
select * from(
select
serial_number
--as we might get more than one offer for a single asset from repair partners, we should use the last offer
,case when right(kva_id,2) in('-1', '-2') then substring(kva_id,0, length(kva_id)-1) else kva_id end as kva_id_new
,row_number() over(partition by kva_id_new,serial_number order by date_offer desc) as rn
from dm_recommerce.repair_cost_estimates ri) where rn=1
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
	left join repair_cost_estimates_pre r
	on u.kva_id = r.kva_id_new
)
,
sn_swap as (
	select
		u.kva_id,
		case when sf.serial_number_fixed is not null then sf.serial_number_fixed
			 when c.new_serial is not null then c.new_serial
			 when sc.serial_number is not null then sc.serial_number
			 else coalesce (u.imei::text, upper(u.serial_number)) end as serial_number ,
		u.imei,
  		case when sf.serial_number_fixed is not null then 'Corrected'
			 when c.new_serial is not null then 'Corrected'
  			 when sc.serial_number is not null then 'Swapped'
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
	left join dm_operations.serial_number_changes sc on sc.old_serial_number = coalesce (u.imei::text, upper(u.serial_number))
	left join recommerce.repair_invoices_sn_fixes sf ON sf.serial_number_before = coalesce (u.imei::text, upper(u.serial_number))--imported from gsheet
	),
enrich_sf as (
	select
		u.kva_id,
		u.serial_number,
		u.imei,
		u.serial_number_before,
		u.swap_status,
		u.repaired_asset_name,
		a.asset_id,
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
	 	repair_price,
		u.repair_partner
	from sn_swap u
	left join master.asset a on u.serial_number = a.serial_number
)
select distinct e.* from enrich_sf e;

GRANT SELECT ON dm_recommerce.repair_invoices TO redash_pricing;
GRANT SELECT ON dm_recommerce.repair_invoices TO tableau;
GRANT SELECT ON dm_recommerce.repair_invoices TO GROUP mckinsey;
