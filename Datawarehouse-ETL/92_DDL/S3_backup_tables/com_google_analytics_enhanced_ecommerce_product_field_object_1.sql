-- S3 Path:
-- Drop table

-- DROP TABLE "atomic".com_google_analytics_enhanced_ecommerce_product_field_object_1;

--DROP TABLE atomic.com_google_analytics_enhanced_ecommerce_product_field_object_1;
CREATE TABLE IF NOT EXISTS atomic.com_google_analytics_enhanced_ecommerce_product_field_object_1
(
	schema_vendor VARCHAR(128) NOT NULL  ENCODE runlength
	,schema_name VARCHAR(128) NOT NULL  ENCODE runlength
	,schema_format VARCHAR(128) NOT NULL  ENCODE runlength
	,schema_version VARCHAR(128) NOT NULL  ENCODE runlength
	,root_id CHAR(36) NOT NULL  ENCODE RAW
	,root_tstamp TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo
	,ref_root VARCHAR(255) NOT NULL  ENCODE runlength
	,ref_tree VARCHAR(1500) NOT NULL  ENCODE runlength
	,ref_parent VARCHAR(255) NOT NULL  ENCODE runlength
	,brand VARCHAR(500)   ENCODE lzo
	,category VARCHAR(500)   ENCODE lzo
	,coupon VARCHAR(500)   ENCODE lzo
	,currency CHAR(3)   ENCODE lzo
	,id VARCHAR(500)   ENCODE lzo
	,list VARCHAR(500)   ENCODE lzo
	,name VARCHAR(500)   ENCODE lzo
	,"position" INTEGER   ENCODE lzo
	,price DOUBLE PRECISION   ENCODE RAW
	,quantity BIGINT   ENCODE lzo
	,variant VARCHAR(500)   ENCODE lzo
)
DISTSTYLE KEY
 DISTKEY (root_id)
 SORTKEY (
	root_tstamp
	)
;
ALTER TABLE atomic.com_google_analytics_enhanced_ecommerce_product_field_object_1 owner to snowplow;


-- "atomic".com_google_analytics_enhanced_ecommerce_product_field_object_1 foreign keys

ALTER TABLE "atomic".com_google_analytics_enhanced_ecommerce_product_field_object_1 ADD CONSTRAINT com_google_analytics_enhanced_ecommerce_product_field_object_1_root_id_fkey FOREIGN KEY (root_id) REFERENCES events(event_id);
