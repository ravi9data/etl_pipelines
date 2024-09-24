

CREATE TABLE IF NOT EXISTS master.addon
(
	addon_id VARCHAR(255)   ENCODE lzo
	,order_id VARCHAR(255)  NOT NULL ENCODE lzo
	,customer_id INTEGER   ENCODE az64
	,related_variant_sku VARCHAR(255)   ENCODE lzo
	,related_product_sku VARCHAR(255)   ENCODE lzo
	,addon_name VARCHAR(255)   ENCODE lzo
	,product_name VARCHAR(255)   ENCODE lzo
	,category_name VARCHAR(65535)   ENCODE lzo
	,subcategory_name VARCHAR(65535)   ENCODE lzo
	,add_on_variant_id VARCHAR(255)   ENCODE lzo
	,country VARCHAR(13)   ENCODE lzo
	,add_on_status VARCHAR(255)   ENCODE lzo
	,order_status VARCHAR(24574)   ENCODE lzo
	,initial_scoring_decision VARCHAR(255)   ENCODE lzo
	,submitted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_amount DOUBLE PRECISION   ENCODE RAW
	,addon_amount NUMERIC(30,2)   ENCODE az64
	,duration INTEGER   ENCODE az64
	,avg_plan_duration DOUBLE PRECISION   ENCODE RAW
	,quantity INTEGER   ENCODE az64
	,PRIMARY KEY (order_id)
)
DISTSTYLE KEY
 DISTKEY (order_id)
;
ALTER TABLE master.addon owner to matillion;