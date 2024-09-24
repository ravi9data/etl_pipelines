DROP TABLE IF EXISTS tmp_bi_ods_variant;
CREATE TEMP TABLE tmp_bi_ods_variant AS 
WITH v AS (
	SELECT
		v.id AS variant_id,
		v.sku AS variant_sku,
		a.name AS variant_name,
  		JSON_EXTRACT_PATH_TEXT(v.properties, 'color', 'name') AS variant_color,
		v.product_id,
		v.updated_at AS variant_updated_at,
		/*consider availability_state only from store - Germany*/
		MAX(CASE WHEN s.automatic=true AND s.store_id =1  THEN 'automatic'
				WHEN s.store_id =1 THEN s.availability
				ELSE NULL END) AS availability_state,
  		a.brand__c AS product_brand,
		v.article_number AS article_number,
		ROW_NUMBER() OVER(PARTITION BY variant_sku ORDER BY variant_updated_at DESC) AS sku_rn
	FROM stg_api_production.spree_variants v
	LEFT JOIN stg_salesforce.product2 a
		ON a.sku_variant__c=v.sku
  	LEFT JOIN stg_api_production.variant_stores s
		ON s.variant_id=v.id 
	INNER JOIN stg_api_production.spree_products p
		ON v.product_id=p.id 
	WHERE true
		--and  store_id='1'
		AND p.category_id IS NOT NULL
		AND p.deleted_at IS NULL
		AND NOT p.deprecated
		AND NOT v.deprecated
		AND v.deleted_at IS NULL
	GROUP BY 1,2,3,4,5,6,8,9
	ORDER BY variant_updated_at DESC)
,b_idx AS (
	SELECT DISTINCT
 		a.f_product_sku_variant__c AS variant_sku,
 		a.name AS variant_name,
 		v2.id AS variant_id,
 		MAX(v2.id) OVER (PARTITION BY v2.sku) AS max_id,
 		v2.product_id,
 		v2.updated_at as variant_updated_at,
 		v.availability_state,
  		v.product_brand,
  		v.article_number,
  		RANK() OVER (PARTITION BY a.f_product_sku_variant__c ORDER BY a.createddate DESC ) AS idx
	FROM stg_salesforce.asset a
	LEFT JOIN stg_salesforce.product2 p
		ON p.sku_variant__c=a.f_product_sku_variant__c
	LEFT JOIN v 
		ON a.f_product_sku_variant__c=v.variant_sku
	LEFT JOIN stg_api_production.spree_variants v2
		ON v2.sku=a.f_product_sku_variant__c
	WHERE v.variant_sku IS NULL)
, b AS (
	SELECT DISTINCT * FROM b_idx WHERE idx = 1 )
	,f AS (
	SELECT
		b.variant_id,
		b.variant_sku,
		b.variant_name,
  		json_extract_path_text(v2.properties, 'color', 'name') AS variant_color,
		b.product_id,
		b.variant_updated_at,
		b.availability_state,
  		b.product_brand,
  		b.article_number
	FROM b
		INNER JOIN stg_api_production.spree_variants v2
		ON v2.id=b.variant_id
	WHERE variant_id = max_id
	UNION ALL
	SELECT
		variant_id,
		variant_sku,
		variant_name,
  		variant_color,
		product_id,
		variant_updated_at,
		availability_state,
  		product_brand,
  		article_number
	FROM v
	WHERE sku_rn = 1)
,eans AS (
	SELECT 
  	DISTINCT
		variant_id
		,LAST_VALUE(VALUE IGNORE NULLS) OVER(PARTITION BY variant_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ean
	FROM stg_api_production.eans
)
,upcs AS (
	SELECT
  	DISTINCT
		variant_id
		,LAST_VALUE(VALUE IGNORE NULLS) OVER(PARTITION BY variant_id ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS upcs
	FROM s3_spectrum_rds_dwh_api_production.upcs
	)
SELECT
		f.variant_id,
		f.variant_sku,
		f.variant_name,
  		INITCAP(CASE WHEN f.variant_color='#000000' THEN 'Black'
        	 WHEN f.variant_color='#FFFFFF' OR f.variant_color='#ffffff' THEN 'White'
            -- when f.variant_color='#AEAEAE' then 'Grey'
        ELSE f.variant_color END) AS variant_color,
		f.product_id,
		f.variant_updated_at,
		f.availability_state,
        f.product_brand,
		COALESCE(split_part(ean,'.',1),NULL) AS ean,
		f.article_number,
		upcs
FROM f
LEFT JOIN eans ON eans.variant_id=f.variant_id
LEFT JOIN upcs ON upcs.variant_id=f.variant_id
;


BEGIN TRANSACTION;

DELETE FROM bi_ods.variant WHERE 1=1;

INSERT INTO bi_ods.variant
SELECT * FROM tmp_bi_ods_variant;

END TRANSACTION;