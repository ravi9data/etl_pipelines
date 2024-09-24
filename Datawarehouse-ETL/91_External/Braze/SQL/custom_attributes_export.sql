/*
This script is to keep the backup and revision history
*/

-- STEP 1: Review Segment

DROP TABLE IF EXISTS dm_marketing.review_segment;
CREATE TABLE dm_marketing.review_segment AS  
SELECT DISTINCT
 	s.customer_id, 
 	current_timestamp AS updated_at,
 	true AS qualified_for_review_invitation
FROM mASter.subscription s 
LEFT JOIN mASter.allocation a ON a.subscription_id = s.subscription_id
LEFT JOIN mASter.ASset ASs ON ASs.ASset_id =a.ASset_id 
WHERE ASs.total_allocations_per_ASset<3 
AND datediff('d',start_date,first_ASset_delivery_date)<=7
GROUP BY 1;


-- STEP 2: Staging export data
DROP TABLE IF EXISTS dm_marketing.stg_braze_export;
CREATE TABLE dm_marketing.stg_braze_export AS
SELECT
	c.customer_id AS external_id,
    --coalesce(pii.email,'N/A') AS email,
    true AS _update_existing_only,
    c.crm_label AS customer_label,
    c.profile_status AS profile_status,
    c.company_status,
    c.customer_type,
    c.company_created_at,
    c.trust_type,
    c.company_type_name AS company_type,
    COALESCE (ts.qualified_for_review_invitation,false) AS qualified_for_review_invitation,
	c.ever_rented_products AS lifetime_rented_product,
	c.ever_rented_categories AS lifetime_rented_category,
	c.ever_rented_subcategories AS lifetime_rented_subcategory,
	c.ever_rented_brands AS lifetime_rented_brand,
	c.ever_rented_sku AS lifetime_rented_sku,
	c.subscription_durations AS lifetime_rental_plan,
    c.voucher_usage AS lifetime_voucher_redeemed,
	c.rfm_segment,
    os.verification_state
FROM master.customer c
--left join ods_data_sensitive.customer_pii pii on pii.customer_id=c.customer_id
LEFT JOIN dm_marketing.review_segment ts ON ts.customer_id=c.customer_id
LEFT JOIN ods_production.order_scoring os ON os.user_id = c.customer_id 
WHERE (c.updated_at) > (SELECT updated_at FROM dm_marketing.braze_date_cntrl);

-- Update date control table 
UPDATE dm_marketing.braze_date_cntrl SET updated_at=(SELECT max(c.updated_at) FROM master.customer c);


-- STEP 3: Creating delta table to send to Braze
DROP TABLE IF EXISTS dm_marketing.braze_export;

CREATE TABLE dm_marketing.braze_export AS 
SELECT 
stg.external_id, 
stg."_update_existing_only", 
stg.customer_label, 
stg.profile_status, 
stg.company_status, 
stg.customer_type, 
stg.company_created_at, 
stg.trust_type, 
stg.company_type, 
stg.qualified_for_review_invitation, 
stg.lifetime_rented_product, 
stg.lifetime_rented_category, 
stg.lifetime_rented_subcategory, 
stg.lifetime_rented_brand, 
stg.lifetime_rented_sku, 
stg.lifetime_rental_plan, 
stg.lifetime_voucher_redeemed, 
stg.rfm_segment, 
stg.verification_state
FROM dm_marketing.stg_braze_export stg
minus
SELECT 
hist.external_id, 
hist."_update_existing_only", 
hist.customer_label, 
hist.profile_status, 
hist.company_status, 
hist.customer_type, 
hist.company_created_at, 
hist.trust_type, 
hist.company_type, 
hist.qualified_for_review_invitation, 
hist.lifetime_rented_product, 
hist.lifetime_rented_category, 
hist.lifetime_rented_subcategory, 
hist.lifetime_rented_brand, 
hist.lifetime_rented_sku, 
hist.lifetime_rental_plan, 
hist.lifetime_voucher_redeemed, 
hist.rfm_segment, 
hist.verification_state
FROM dm_marketing.braze_export_history hist;


-- STEP 4: Historical table, for a backtup to S3
-- Deleting previous records

DELETE FROM dm_marketing.braze_export_history
USING dm_marketing.stg_braze_export
WHERE dm_marketing.braze_export_history."external_id" = dm_marketing.stg_braze_export."external_id";

-- Insert new records

INSERT INTO dm_marketing.braze_export_history
SELECT 
external_id, 
"_update_existing_only", 
customer_label, 
profile_status, 
company_status, 
customer_type, 
company_created_at, 
trust_type, 
company_type, 
qualified_for_review_invitation, 
lifetime_rented_product, 
lifetime_rented_category, 
lifetime_rented_subcategory, 
lifetime_rented_brand, 
lifetime_rented_sku, 
lifetime_rental_plan, 
lifetime_voucher_redeemed, 
rfm_segment, 
verification_state
FROM dm_marketing.stg_braze_export;

-- STEP 5: Dropping intermediate tables
DROP TABLE dm_marketing.review_segment;

