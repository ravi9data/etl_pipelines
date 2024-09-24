DROP TABLE IF EXISTS tmp_inventory_reservation;

CREATE TEMP TABLE tmp_inventory_reservation (LIKE ods_production.inventory_reservation);

INSERT INTO tmp_inventory_reservation
with
parsing as (
select
	json_extract_path_text(json_extract_path_text(payload, 'state'),'uid') as uid,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'customerId') as customer_id,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'skuVariantCode') as sku_variant_code,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'stockUid') as stock_uid,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'storeId') as store_id,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'orderId') as order_id,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'orderNumber') as order_number,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'orderMode') as order_mode,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'customerType') as customer_type,
	--json_extract_path_text(json_extract_path_text(payload, 'state'),'isActive') as isActive ,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'status') as status,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'initialQuantity') as initial_quantity,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'quantity') as quantity,
	nullif(nullif(json_extract_path_text(json_extract_path_text(payload, 'state'),'deletedAt'), '') , '{}') as deleted_at,
	nullif(nullif(json_extract_path_text(json_extract_path_text(payload, 'state'),'createdAt'), '') , '{}') as created_at,
	coalesce(
	coalesce(
		nullif(nullif(json_extract_path_text(json_extract_path_text(payload, 'state'),'updatedAt'), '') , '{}')
		, kafka_received_at), consumed_at) as updated_at,
	row_number() over (partition by uid order by updated_at desc) rn
from s3_spectrum_kafka_topics_raw.inventory_reservation s
WHERE  cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-2
),
transposing as (
select
	uid,
	customer_id,
	sku_variant_code ,
	stock_uid,
	store_id,
	order_id,
	order_number,
	order_mode,
	customer_type,
	--isactive ,
	status,
	initial_quantity ,
	quantity ,
	rn,
	max(created_at) over (partition by uid) as created_at,
	max(deleted_at) over (partition by uid) as deleted_at,
	max(case when status = 'pending' then updated_at end) over (partition by uid) pending_at,
	max(case when status = 'confirmed' then updated_at end) over (partition by uid) confirmed_at,
	max(case when status = 'approved' then updated_at end) over (partition by uid) approved_at,
	max(case when status = 'paid' then updated_at end) over (partition by uid) paid_at,
	max(case when status = 'declined' then updated_at end) over (partition by uid) declined_at,
	max(case when status = 'cancelled' then updated_at end) over (partition by uid) cancelled_at,
	max(case when status = 'expired' then updated_at end) over (partition by uid) expired_at,
	max(case when status = 'fulfilled' then updated_at end) over (partition by uid) fulfilled_at
From parsing)
select
	uid,
	customer_id,
	sku_variant_code ,
	stock_uid,
	store_id,
	order_id,
	order_number,
	order_mode,
	customer_type,
	case when 	deleted_at is not null or 
				declined_at is not null or
				cancelled_at is not null or 
				expired_at is not null or
				fulfilled_at is not null 
		 then 'false' else 'true' end as isactive ,
	status,
	initial_quantity ,
	quantity ,
	created_at::timestamp,
	deleted_at::timestamp,
	pending_at::timestamp,
	confirmed_at::timestamp,
	approved_at::timestamp,
	paid_at::timestamp,
	declined_at::timestamp,
	cancelled_at::timestamp,
	expired_at::timestamp,
	fulfilled_at::timestamp
from transposing
where rn = 1;

INSERT INTO ods_production.inventory_reservation
SELECT DISTINCT tir.*
FROM tmp_inventory_reservation tir
LEFT JOIN ods_production.inventory_reservation ir
	ON ir.uid = tir.uid
WHERE ir.uid IS NULL;

UPDATE
	ods_production.inventory_reservation
SET
	customer_id =tir.customer_id,
	sku_variant_code  =tir.sku_variant_code,
	stock_uid =tir.stock_uid,
	store_id =tir.store_id,
	order_id =tir.order_id,
	order_number =tir.order_number,
	order_mode =tir.order_mode,
	customer_type =tir.customer_type,
	isactive =tir.isactive,
	status =tir.status,
	initial_quantity =tir.initial_quantity,
	quantity =tir.quantity,
	created_at = CASE WHEN tir.created_at IS NOT NULL THEN tir.created_at ELSE inventory_reservation.created_at end,
	deleted_at =CASE WHEN tir.deleted_at IS NOT NULL THEN tir.deleted_at ELSE inventory_reservation.deleted_at end,
	pending_at =CASE WHEN tir.pending_at IS NOT NULL THEN tir.pending_at ELSE inventory_reservation.pending_at end,
	confirmed_at =CASE WHEN tir.confirmed_at IS NOT NULL THEN tir.confirmed_at ELSE inventory_reservation.confirmed_at end,
	approved_at =CASE WHEN tir.approved_at IS NOT NULL THEN tir.approved_at ELSE inventory_reservation.approved_at end,
	paid_at =CASE WHEN tir.paid_at IS NOT NULL THEN tir.paid_at ELSE inventory_reservation.paid_at end,
	declined_at =CASE WHEN tir.declined_at IS NOT NULL THEN tir.declined_at ELSE inventory_reservation.declined_at end,
	cancelled_at =CASE WHEN tir.cancelled_at IS NOT NULL THEN tir.cancelled_at ELSE inventory_reservation.cancelled_at end,
	expired_at =CASE WHEN tir.expired_at IS NOT NULL THEN tir.expired_at ELSE inventory_reservation.expired_at end,
	fulfilled_at =CASE WHEN tir.fulfilled_at IS NOT NULL THEN tir.fulfilled_at ELSE inventory_reservation.fulfilled_at end
FROM tmp_inventory_reservation tir
where inventory_reservation.uid = tir.uid;
