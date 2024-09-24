with 
reporting_date (_period) as (
	select 
		to_char(dateadd('month',-1,current_date), 'YYYYMM') --YYYYMM format
	),
raw_cn8 as (
	--- flag if the cn8 code is included in Dutch gov. list, consider that cn8 code first
	--- if multiple official codes exist, then choose most common one 
	--- if no cn8 code is available in the list, then again choose most common one
	select 
		sku,
		cn8_code ,
		case when exist_in_official = 'TRUE' then 1 else 0 end exist_in_official ,
		sp_unit,
		count(1) common_rank
	from public.cn8_list
	group by 1,2,3,4
	),
cn8_list as (
	select 
		sku,
		cn8_code ,
		sp_unit ,
		exist_in_official
	from raw_cn8 
	where 1=1 --to run the script in dbeaver without error
	qualify row_number () over (partition by sku order by exist_in_official desc, common_rank desc) = 1
),
first_wh_l as (
	select 
		asset_id , 
		warehouse ,
		capital_source_name
	from master.asset_historical 
	where purchase_request_item_id is not null 
	and asset_id in (select asset_id from master.asset where created_at > '2022-07-31')
	and variant_sku not in (
			'GRB224P17689V30031' -- Apple MDM Activation - ADDS ON
		)
	qualify row_number () over (partition by asset_id order by date asc ) = 1
),
_final as (
	select 
		'IGV' as headline,
		to_char(a.created_at, 'YYYYMM') as "period" , 
		6 as commodity_flow,
		null as partner_id ,
		null as country_of_destination,
		sz.country_code  as country_of_origin,
		11 as nature_of_transaction,
		3 as mode_of_transport,
		null as delivery_terms,
		c.cn8_code,
		case when c.sp_unit = '-' then null::text 
			 when c.sp_unit = 'p/st' then '1'
			 else c.sp_unit end as supplementary_unit,
		case when supplementary_unit = 1 
			 then null
			 else 	 
			 	greatest(case when d.is_weight_missing 
					 		  then d.category_weight 
					 		  else d.original_weight end ,
					 	 1)
			 end as net_mass,
		a.initial_price as invoice_value,
		null as currency_code,
		null as invoice_value_foreign,
		a.serial_number as administration_number, 
		a.supplier ,
		a.product_sku as sku,
		w.capital_source_name,
		pr.warehouse__c ,
		a.category_name ,
		a.subcategory_name ,
		a.brand ,
		a.asset_name ,
		c.exist_in_official
	from master.asset a 
	left join first_wh_l w 
		on a.asset_id = w.asset_id
	left join stg_salesforce.purchase_request__c pr 
		on a.request_id  = pr.name 
	left join ods_production.supplier sp 
		on pr.supplier__c = sp.supplier_id 
	left join staging_airbyte_bi.supplier_zipcodes sz 
		on sz.supplier_name = a.supplier
	left join dm_operations.dimensions d 
		on a.variant_sku = d.variant_sku
	left join cn8_list c
		on --replace(ltrim(replace(c.ean,'0','')),'','0') = replace(ltrim(replace(a.ean,'0','')),'','0')--c.ean = a.ean 
		 c.sku = a.product_sku  
	where a.purchase_request_item_id is not null
	and w.warehouse = 'ups_softeon_eu_nlrng'
	and "period" = (select _period from reporting_date) 
	-- let's also include if country_code is missing, because it must be a new supplier 
	-- so we can add new supplier info to source sheet (later in the checks)
	and (sz.country_code is null or (sz.eu_non_eu = 'EU' and sz.country_code <> 'NL'))
	order by cn8_code asc, sku
),
checks as (
	---check missing and if it exists in official list
	select distinct supplier, category_name , subcategory_name , brand, sku, asset_name  
	from _final
	where cn8_code is null
),
export as (
	select 
		"period",
		commodity_flow ,
		country_of_origin ,
		nature_of_transaction ,
		mode_of_transport ,
		cn8_code ,
		net_mass ,
		supplementary_unit ,
		invoice_value ,
		currency_code,
		invoice_value_foreign ,
		administration_number 
	from _final 
)
---
---
---
---
-- 1. CHECK MISSINGS CN8 CODE
 select * from checks 
---
---
-- 2. CHECK IF THERE IS NEW SUPPLIER
--select distinct supplier, country_of_origin from _final where country_of_origin is null
---
---
--- IF NO MISSING, THEN COMMENT OUT ABOVE 
--- AND RUN THE SCRIPT BELOW
--select * from export;