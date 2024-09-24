-- master.allocation definition

-- Drop table

-- DROP TABLE master.allocation;

--DROP TABLE master.allocation;
CREATE TABLE IF NOT EXISTS master.allocation
(
	allocation_id VARCHAR(50) NOT NULL  ENCODE zstd
	,asset_id VARCHAR(18)   ENCODE zstd
	,allocation_sf_id VARCHAR(80)   ENCODE zstd
	,subscription_id VARCHAR(18)   ENCODE zstd
	,customer_type VARCHAR(65535)   ENCODE zstd
	,store_type VARCHAR(7)   ENCODE zstd
	,store_short VARCHAR(20)   ENCODE zstd
	,subcategory_name VARCHAR(65535)   ENCODE zstd
	,product_sku VARCHAR(255)   ENCODE zstd
	,allocation_status_original VARCHAR(255)   ENCODE lzo
	,is_manual_allocation VARCHAR(9)   ENCODE zstd
	,is_recirculated VARCHAR(13)   ENCODE zstd
	,is_last_allocation_per_asset BOOLEAN   ENCODE RAW
	,rank_allocations_per_subscription BIGINT   ENCODE zstd
	,order_approved_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_completed_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,subscription_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,allocated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,push_to_wh_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ready_to_ship_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_label_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_provider VARCHAR(1300)   ENCODE zstd
	,picked_by_carrier_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,shipment_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_delivery_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_reason VARCHAR(65535)   ENCODE zstd
	,is_package_lost BOOLEAN   ENCODE RAW
	,delivered_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,return_shipment_label_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,return_shipment_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,return_delivery_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancellation_returned_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refurbishment_start_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refurbishment_end_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,revocation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,shipment_tracking_number VARCHAR(65535)   ENCODE lzo
	,return_shipment_tracking_number VARCHAR(65535)   ENCODE lzo,
	,shipping_country VARCHAR(65535)   ENCODE lzo
	,city VARCHAR(256)   ENCODE lzo
	,state_name VARCHAR(256)   ENCODE lzo,
	,replacement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,replaced_by VARCHAR(16383)   ENCODE lzo
	,replacement_for VARCHAR(16383)   ENCODE lzo
	,replacement_reason VARCHAR(255)   ENCODE lzo
	,PRIMARY KEY (allocation_id)
)
DISTSTYLE KEY
 DISTKEY (subscription_id)
 SORTKEY (
	updated_at
	)
;
ALTER TABLE master.allocation owner to matillion;
