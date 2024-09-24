-- master.variant definition

-- Drop table

-- DROP TABLE master.variant;

--DROP TABLE master.variant;
CREATE TABLE IF NOT EXISTS master.variant
(
	variant_id INTEGER NOT NULL  ENCODE az64
	,variant_sku VARCHAR(1300)   ENCODE zstd
	,variant_name VARCHAR(255)   ENCODE zstd
	,variant_color VARCHAR(65535)   ENCODE zstd
	,variant_updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,availability_state VARCHAR(65535)   ENCODE zstd
	,ean VARCHAR(16)   ENCODE zstd
	,last_mm_price VARCHAR(500)   ENCODE zstd
	,product_id INTEGER   ENCODE az64
	,product_sku VARCHAR(255)   ENCODE zstd
	,product_created TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,product_name VARCHAR(510)   ENCODE zstd
	,category_name VARCHAR(65535)   ENCODE zstd
	,subcategory_name VARCHAR(65535)   ENCODE zstd
	,brand VARCHAR(65535)   ENCODE zstd
	,pdp_url VARCHAR(510)   ENCODE zstd
	,market_price NUMERIC(12,2)   ENCODE az64
	,rank INTEGER   ENCODE az64
	,rental_plans VARCHAR(65535)   ENCODE zstd
	,pending_allocation_mm BIGINT   ENCODE az64
	,assets_stock_mm BIGINT   ENCODE az64
	,assets_stock_mm_new BIGINT   ENCODE az64
	,assets_stock_mm_agan BIGINT   ENCODE az64
	,assets_purchased_last_3months_mm BIGINT   ENCODE az64
	,assets_book_mm BIGINT   ENCODE az64
	,requested_mm DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_mm BIGINT   ENCODE az64
	,pending_allocation_saturn BIGINT   ENCODE az64
	,assets_stock_saturn BIGINT   ENCODE az64
	,assets_stock_saturn_new BIGINT   ENCODE az64
	,assets_stock_saturn_agan BIGINT   ENCODE az64
	,assets_purchased_last_3months_saturn BIGINT   ENCODE az64
	,requested_saturn DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_saturn BIGINT   ENCODE az64
	,pending_allocation_conrad BIGINT   ENCODE az64
	,assets_stock_conrad BIGINT   ENCODE az64
	,requested_conrad DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_conrad BIGINT   ENCODE az64
	,pending_allocation_gravis BIGINT   ENCODE az64
	,assets_stock_gravis BIGINT   ENCODE az64
	,requested_gravis DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_gravis BIGINT   ENCODE az64
	,pending_allocation_unito BIGINT   ENCODE az64
	,assets_stock_quelle BIGINT   ENCODE az64
	,requested_quelle DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_unito BIGINT   ENCODE az64
	,pending_allocation_weltbild BIGINT   ENCODE az64
	,assets_stock_weltbild BIGINT   ENCODE az64
	,requested_weltbild DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_weltbild BIGINT   ENCODE az64
	,pending_allocation_alditalk BIGINT   ENCODE az64
	,assets_stock_alditalk BIGINT   ENCODE az64
	,requested_alditalk DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_alditalk BIGINT   ENCODE az64
	,pending_allocation_comspot BIGINT   ENCODE az64
	,assets_stock_comspot BIGINT   ENCODE az64
	,requested_comspot DOUBLE PRECISION   ENCODE RAW
	,approved_pending_manual_review_comspot BIGINT   ENCODE az64
	,pending_allocation_shifter BIGINT   ENCODE az64
	,assets_stock_shifter BIGINT   ENCODE az64
	,requested_shifter BIGINT   ENCODE az64
	,approved_pending_manual_review_shifter DOUBLE PRECISION   ENCODE zstd
    ,pending_allocation_irobot BIGINT   ENCODE az64
	,assets_stock_irobot BIGINT   ENCODE az64
	,requested_irobot BIGINT   ENCODE az64
	,approved_pending_manual_review_irobot DOUBLE PRECISION   ENCODE zstd
	,pending_allocation_samsung BIGINT   ENCODE az64
	,assets_stock_samsung BIGINT   ENCODE az64
	,requested_samsung DOUBLE PRECISION   ENCODE zstd
	,approved_pending_manual_review_samsung BIGINT   ENCODE az64
	,pending_allocation_others BIGINT   ENCODE az64
	,assets_stock_others BIGINT   ENCODE az64
	,requested_others DOUBLE PRECISION   ENCODE zstd
	,approved_pending_manual_review_others BIGINT   ENCODE az64
	,avg_rental_duration DOUBLE PRECISION   ENCODE zstd
	,acquired_subscriptions BIGINT   ENCODE az64
	,acquired_subs_last_3months BIGINT   ENCODE az64
	,active_subs BIGINT   ENCODE az64
	,active_subs_value DOUBLE PRECISION   ENCODE zstd
	,net_revenue_paid DOUBLE PRECISION   ENCODE zstd
	,total_investment DOUBLE PRECISION   ENCODE zstd
	,total_assets_qty BIGINT   ENCODE az64
	,investment_last_3months DOUBLE PRECISION   ENCODE zstd
	,assets_purchased_last_3months BIGINT   ENCODE az64
	,avg_purchase_price DOUBLE PRECISION   ENCODE zstd
	,avg_purchase_price_last_3months DOUBLE PRECISION   ENCODE zstd
	,assets_on_loan BIGINT   ENCODE az64
	,assets_in_stock BIGINT   ENCODE az64
	,assets_on_debt_collection BIGINT   ENCODE az64
	,submitted_orders BIGINT   ENCODE az64
	,paid_orders BIGINT   ENCODE az64
	,cancelled_orders BIGINT   ENCODE az64
	,declined_orders BIGINT   ENCODE az64
	,PRIMARY KEY (variant_id)
)
DISTSTYLE KEY
 DISTKEY (variant_sku)
;
ALTER TABLE master.variant owner to matillion;