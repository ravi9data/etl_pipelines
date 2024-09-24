-- "atomic".com_google_analytics_enhanced_ecommerce_action_1 definition
-- Drop table

-- DROP TABLE "atomic".com_google_analytics_enhanced_ecommerce_action_1;

--DROP TABLE atomic.com_google_analytics_enhanced_ecommerce_action_1;
CREATE TABLE IF NOT EXISTS atomic.com_google_analytics_enhanced_ecommerce_action_1
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
	,"action" VARCHAR(15)   ENCODE lzo
)
DISTSTYLE KEY
 DISTKEY (root_id)
 SORTKEY (
	root_tstamp
	)
;
ALTER TABLE atomic.com_google_analytics_enhanced_ecommerce_action_1 owner to snowplow;


-- "atomic".com_google_analytics_enhanced_ecommerce_action_1 foreign keys

ALTER TABLE "atomic".com_google_analytics_enhanced_ecommerce_action_1 ADD CONSTRAINT com_google_analytics_enhanced_ecommerce_action_1_root_id_fkey FOREIGN KEY (root_id) REFERENCES events(event_id);
