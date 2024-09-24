DROP TABLE IF EXISTS dm_marketing.braze_campaign_mapping; 
CREATE TABLE dm_marketing.braze_campaign_mapping
AS
SELECT DISTINCT campaign_id , campaign_name , message_variation_id , message_variation_name
FROM stg_external_apis.braze_campaign_details;



