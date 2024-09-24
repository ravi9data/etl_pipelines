drop table if exists dm_recommerce.return_stages;
create table dm_recommerce.return_stages as

with 
wemalo_data_s1 as(
select articleid,
to_timestamp(event_timestamp, 'YYYY-MM-DD HH24:MI:SS') as event_timestamp,
cell,
--if there is any serialnumber changes, use the last serialnumber 
max(case when serialnumber <>refurbishmententry_serialnumber then split_part(lower(refurbishmententry_serialnumber), '=>', 2) else serialnumber  end)
over(partition by articleid) as serial_number
from stg_kafka_events_full.stream_wms_wemalo_register_refurbishment swwr --where serialnumber ='001136301557'--where articleid  ='15233340'
),

wemalo_data_s2 as(
	select wd.event_timestamp,   
	lower(cell) as cell_,
	wd.serial_number,
	wd.articleid,
	case when split_part(cell_, '-', 1) = 'spl' and split_part(cell_, '-', 2) = 'dr' then 'spl-dr' 
		 else split_part(cell_, '-', 1) end as cell_type
	from wemalo_data_s1 wd
),


wemalo_data_s3 as(
select wd.*,
case when --normal return or direct return
		       cell_type in ('ret1', 'spl-dr', 'new') 
			    or 
			   cell_type like 'kom%'
				 --if it is failed delivery, highly possible that asset returns to position starts with kom/new
			then 'scanned' else lower(sperrlager_type) end as ref_status,
--rn is created to use first event if there is more than 1 event for same position of an article
row_number() over(partition by articleid, serial_number, ref_status order by event_timestamp asc ) as rn
from wemalo_data_s2 wd
left join stg_external_apis.wemalo_position_labels l on wd.cell_ = lower(l.position)
),

wemalo_data_final as(
select event_timestamp,
serial_number,
articleid,
ref_status,
	case  when ref_status ='scanned' then 'Incoming'
		  when ref_status in(
							'cleanup',
							'incomplete',
							'locked',
							'reconciliation out',
							'repair',
							'repair incoming',
							'sn mismatch',
							'scooter licence',
							'undefined',
							'warranty',
							'transfer to ingram'
							) then 'In Process'
			when ref_status in(
							'b2b bulky',
							'irrepearable (optical)',
							'irrepearable (technical)',
							'lost',
							'replenishment',
							'sperlagger lüneburg',
							'stock',
							'sell'
							) then 'Processed' end as return_stages,
	case when ref_status ='scanned' then 'scanned'
		when ref_status in(
							'incomplete',
							'locked',
							'sn mismatch',
							'scooter licence',
							'cleanup'
							) then 'grading'
		 when ref_status='lost' then 'lost'
		 when ref_status in(
						 	'repair',
							'repair incoming',
							'warranty'
		 					) then 'repair'
		 when ref_status  in(
					 	 	'b2b bulky',
							'irrepearable (optical)',
							'irrepearable (technical)',
							'sell'
		 	 				) then 'recommerce'
		 when ref_status  in(
							'replenishment',
							'sperlagger lüneburg',
							'stock'
							) then 'in stock'
			else null end as return_stages_detailed,
    'wemalo' as _3pl

from wemalo_data_s3 where rn=1
	),
	
ingram_base_data as(
select date_of_processing,
serial_number,
disposition_code,
order_number,
status_code from(
select ii.source_timestamp as date_of_processing,
ii.serial_number,
ii.disposition_code,
ii.status_code,
ii.order_number,
--rn is created to use first event if there is more than 1 event for same position of an order_number
row_number() over(partition by order_number, serial_number, disposition_code, status_code order by source_timestamp) as rn
from recommerce.ingram_micro_send_order_grading_status ii) where rn=1 --and serial_number ='001136301557'
),
	
ingram_data as(
select ii.date_of_processing,
ii.serial_number,
order_number as articleid,
ii.disposition_code || '--> ' || ii.status_code as ref_status,
case when steps_4r ='In process return funnel' then 'In Process'
     when steps_4r ='Incoming return funnel' then 'Incoming'
     when (steps_4r ='Processed return funnel' or steps_4r='Incoming recommerce') then 'Processed'
     else 'Recommerce' end as return_stages,
null::text  as return_stages_detailed,
'Ingram'    as _3pl
from ingram_base_data ii
left join recommerce.ingram_steps is_ on ii.status_code = is_.status_code and ii.disposition_code =is_.disposition_code
),

union_wemalo_ingram_base as(
select *,
--if an asset transfer to Ingram, use the last article_id in Wemalo until new Incoming event in Ingram
--if the article_id is null, the reason is there is no Incoming event in Ingram for the corresponding asset
coalesce ((min(case when return_stages='Incoming' and _3pl='Ingram' then event_timestamp end ) over(partition by serial_number)), current_date) as first_incoming_date_in_ingram,
max(case when ref_status='transfer to ingram' then articleid end) over(partition by serial_number) as last_article_id_in_wemalo,
case when event_timestamp<first_incoming_date_in_ingram and _3pl='Ingram' then last_article_id_in_wemalo else articleid end article_id --e.g.serial_number='002024610359'
from(
select * from wemalo_data_final
union
select * from ingram_data)),

union_wemalo_ingram as(
select *,
listagg(distinct return_stages, ',') within group(order by event_timestamp)
    over(partition by serial_number, article_id) as return_stages_all
from union_wemalo_ingram_base
),

salesforce_data as(
select a.asset_id,
a.serial_number as serialnumber,
a.asset_name,
a.category_name,
a.subcategory_name,
a.brand,
a.product_sku,
a.variant_sku,
a.created_at,
a.initial_price,
a.residual_value_market_price,
a.subscription_revenue
from master.asset a 
),

enriching_with_SF_data as(
select w.event_timestamp,
w.article_id,
w.ref_status,
w._3pl,
w.return_stages_all,
w.return_stages,
w.return_stages_detailed,
w.serial_number,
--has_scanned_position field is used to exclude the assets which don't have scanned event
case when return_stages_all like '%Incoming%' then true else false end as has_scanned_position,
(min(case when return_stages='Incoming' then event_timestamp end) over(partition by serial_number, article_id))::date as in_process_start_date,
coalesce((min(case when return_stages='Processed' then event_timestamp end) over(partition by serial_number, article_id))::date, current_date) as in_process_end_date,
(max(case when return_stages='Processed' then event_timestamp end) over(partition by serial_number, article_id))::date as processed_date,
--row_number() over(partition by serial_number,article_id, return_stages) as rn, 
s.*
from union_wemalo_ingram w
left join salesforce_data s on w.serial_number=s.serialnumber
)

select *
from enriching_with_SF_data sf;

GRANT SELECT ON dm_recommerce.return_stages TO tableau;
