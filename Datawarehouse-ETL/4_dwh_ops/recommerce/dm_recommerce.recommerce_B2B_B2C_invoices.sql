drop table if exists dm_recommerce.recommerce_B2B_B2C_invoices;
create table dm_recommerce.recommerce_B2B_B2C_invoices as
with union_b2b_b2c as(
select 
'B2C'   			as invoice_type,
fi.inventory_id			as inventory_id,
fi.payment_date	 		as sold_date,
fi.channel				as channel,
null::int			    as partner_id,
null::int 				as batch,
retail_price_gross 			as sales_price_gross,
fi.total_retail_price_net 	as sales_price_net,
''						as damage_detail,
fi.total_payout 		as total_payout,  
fi.total_billing 		as total_billing,
fi.total_opex	 		as total_opex,
fi.total_rev_share  	as total_rev_share
from recommerce.foxway_invoices fi
union all
select 
'B2B'    				as invoice_type,
serialnumber 				as serial_number,
''					as inventory_id,
to_timestamp(('20'||right(b.date_created,2) ||'-'||substring(b.date_created,4,2)||'-'||left(b.date_created,2)), 'YYYY-MM-DD HH24:MI:SS')::date as sold_date,
'' 					as channel,
partner				as partner_id,
batch 				as batch,
replace(replace(b.sold_price_gross, ',', '.'),' ', '')::float 	as sales_price_gross,
replace(b.sold_price_net, ',', '.')::float 	as sales_price_net,
b.damage as damage_detail,	
null::float 			 as total_payout,
null::float  			 as total_billing,
null::float	  			 as total_opex,
null::float 			 as total_rev_share
from s3_spectrum_recommerce.repair_partner_b2b b
where sold_price_gross<>'08.11.21' --since sold_price_gross is string, there are some incorrect values, exclude them
and sold_price_gross<>'28.02.22'
and sold_price_gross<>'#N/A' 
and sold_price_gross<>''
and date_created <>'92,85'
),
master_ah as (
select distinct
	asset_id,
	asset_name ,
	ah.serial_number,
	brand,
	category_name,
	subcategory_name,
	initial_price, 
	residual_value_market_price,
	date
from master.asset_historical ah 
where ah.residual_value_market_price > 0
and (ah.serial_number, ah.date) in (select distinct serial_number, dateadd('month',-1,date_trunc('month', sold_date)) from union_b2b_b2c)
)
select u.*,
p.name as partner,
m.asset_id,
m.asset_name,
m.brand,
m.category_name,
m.subcategory_name,
m.initial_price, 
m.residual_value_market_price
from union_b2b_b2c u
left join master_ah m 
	on u.serial_number = m.serial_number 
		and m.date = dateadd('month',-1,date_trunc('month', u.sold_date))
left join recommerce.partners p on u.partner_id = p.partner_id;

GRANT SELECT ON dm_recommerce.recommerce_B2B_B2C_invoices TO tableau;
