delete from recommerce.ingram_inventory
where snapshot_date = current_date;

INSERT INTO recommerce.ingram_inventory
SELECT
	row_number() OVER (ORDER BY order_number),
	"order_number",
	"serial_number",
	"item_number",
	"shipping_number",
	"disposition_code",
	"status_code",
	NULLIF("timestamp",'NaT')::timestamp without time zone,
	"partner_id",
	"asset_serial_number",
	"package_serial_number",
	"questions",
	"shipment_type",
	"date_of_processing"::timestamp without time zone,
	current_date AS snapshot_date
FROM
	staging.airbyte_ingram_warehouse_airbyte_do_not_change;


/************ ingram_inventory_partner_and_location_code_details *************/
drop table if exists recommerce.ingram_inventory_partner_and_locationcode_details;
create table recommerce.ingram_inventory_partner_and_locationcode_details as
with latest_record_of_the_day as(
  select
    work_order_id,
    serial_no_,
    location_code,
    processing_date
  from	(
      select
        work_order_id,
        serial_no_,
        location_code,
        extracted_at::date as processing_date,
        row_number() over(partition by work_order_id,extracted_at::date, location_code order by extracted_at desc, location_code desc) as rn,
        row_number() over(partition by work_order_id,extracted_at::date order by extracted_at desc) as rn_1
      from s3_spectrum_recommerce.ingram_micro_grading_order)
      where rn =1 and rn_1=1
  ),

ingram_inventory_partner_and_locationcode_detail as(
  select ii.*,
      p.name as partner_name,
      case when disposition_code is null
  		   then 'null' else disposition_code
  		   end as disposition_code_new,
      lr.location_code,
      case when disposition_code='RECOMMERCE' and status_code='AVAILABLE, RECOMMERCE' and location_code is null then 'null'
           when disposition_code='RECOMMERCE' and status_code='AVAILABLE, RECOMMERCE' and location_code is not null then location_code
    	   else 'null' end as location_code_new
  from recommerce.ingram_inventory ii
  left join latest_record_of_the_day lr on ii.order_number = lr.work_order_id and lr.processing_date=ii.date_of_processing
  left join recommerce.partners p on ii.partner_id = p.partner_id_ingram
)
select
	id.*,
    m.kpi,
    m.stages
from ingram_inventory_partner_and_locationcode_detail id
left join recommerce.ingram_kpi_mapping m on id.disposition_code_new=m.disposition_code
and id.status_code=m.status_code and id.location_code_new=m.location_code_b2b_and_b2b_bulky;


GRANT SELECT ON TABLE recommerce.ingram_inventory_partner_and_locationcode_details TO tableau;
GRANT SELECT ON TABLE recommerce.ingram_inventory_partner_and_locationcode_details TO GROUP bi;
GRANT SELECT ON TABLE recommerce.ingram_inventory_partner_and_locationcode_details TO bart;
GRANT SELECT ON recommerce.ingram_inventory_partner_and_locationcode_details TO tableau;
