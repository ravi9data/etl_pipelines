drop table if exists dm_recommerce.repair_cost_estimates;
create table dm_recommerce.repair_cost_estimates as

with union_all as (
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'finito' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_finito
	union all
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		round(replace(tax , ',', '.'), 0) tax,
		date_offer,
		date_updated,
		decision,
		'gravis' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_gravis
	union all
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'letmerepair' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_letmerepair
	union all
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'mttc' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_mttc
	union all
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'refixo' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_refixo
	union all
	select 
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_total as price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'sertec' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_sertec
	union all
	select
		reverse (split_part (reverse("$path"), '/', 1) ) file_name, 
		kva_id,
		serialnumber,
		imei,
		device,
		description,
		quantity,
		price_netto,
		price_brutto,
		tax,
		date_offer,
		date_updated,
		decision,
		'tmx' as repair_partner 
	from s3_spectrum_recommerce.repair_partner_tmx
	),
stg_sftp as (
	select 
		file_name, 	
		kva_id ,
	  	--' ' is for removing vertical tabs, this is not a space 
		case when repair_partner ='gravis' and left(upper(replace(serialnumber, ' ', '')), 1) <> 'S' 
		then 'S' || upper(replace(serialnumber, ' ', ''))
		else upper(replace(serialnumber, ' ','')) end as serialnumber ,
		nullif(replace(imei, ' ',''), '')::text as imei,
		device ,
		description ,
		quantity ,
		replace(replace(price_netto, ',', '.'),' €','')::float price_netto,
		replace(replace(price_brutto , ',', '.'),' €','')::float price_brutto,
		tax::float,
		case when length(date_offer) = 10 then to_date(date_offer, 'dd.mm.yyyy') 
			 when length(date_offer) = 8  then to_date(date_offer, 'dd.mm.yy') end as date_offer,
		case when length(date_updated) = 10 then to_date(date_updated, 'dd.mm.yyyy') 
		 	 when length(date_updated) = 8  then to_date(date_updated, 'dd.mm.yy') end as date_updated,
		case when trim(upper(decision)) = '' then 'Irreperable'
		   	 when trim(upper(decision)) like '%NO%' then 'No Repair'
		        else 'Repair' end as decision ,
		repair_partner  
	from union_all
	where nullif(kva_id, '') is not null
	),
sn_swap as (
	select
		r.file_name ,
		r.kva_id ,
		case when c.new_serial is not null then c.new_serial 
			 when sns.new_serial is not null then sns.new_serial 
			 else coalesce (r.imei::text, upper(r.serialnumber)) end as serial_number ,
		r.imei,
  		case when c.new_serial is not null then 'Corrected'
  			 when sns.new_serial is not null then 'Swapped'
  			 else 'Original' end as swap_status,
		r.device,
		r.description ,
		r.quantity ,
		r.price_netto ,
		r.price_brutto ,
		r.tax,
		r.date_offer ,
		r.date_updated ,
		r.decision ,
		r.repair_partner 
	from stg_sftp r 
	left join dm_recommerce.corrected_sn c on c.old_serial = coalesce (r.imei::text, upper(r.serialnumber))
	left join dm_recommerce.serial_number_swap sns on sns.old_serial = coalesce (r.imei::text, upper(r.serialnumber)) 
	),
enrich_sf as (
	select 
		s.file_name ,
		s.kva_id ,
		s.serial_number ,
		s.imei ,
  	    s.swap_status,
		s.device ,
		a.asset_id,
		a.asset_name ,
		a.category_name ,
		a.subcategory_name ,
		a.brand ,
		a.product_sku ,
		a.variant_sku ,
		s.description ,
		s.quantity ,
		s.price_netto ,
		s.price_brutto,
		s.tax,
		s.date_offer ,
		s.date_updated ,
		s.decision ,
		s.repair_partner 
	from sn_swap s
	left join master.asset a on a.serial_number = s.serial_number 
)
select * from enrich_sf ;

GRANT SELECT ON dm_recommerce.repair_cost_estimates TO redash_pricing,hightouch_pricing;

GRANT SELECT ON dm_recommerce.repair_cost_estimates TO tableau;
