DROP TABLE IF EXISTS tmp_bi_ods_variant;
CREATE TEMP TABLE tmp_bi_ods_variant AS 
with v as (
	select
		v.id as variant_id,
		v.sku as variant_sku,
		a.name as variant_name,
  		json_extract_path_text(v.properties, 'color', 'name') as variant_color,
		v.product_id,
		v.updated_at as variant_updated_at,
		/*consider availability_state only from store - Germany*/
		max(case when s.automatic=true AND s.store_id =1  then 'automatic'
				WHEN s.store_id =1 THEN s.availability
				ELSE NULL END) as availability_state,
  		a.brand__c as product_brand,
		v.article_number as article_number,
		row_number() over(partition by variant_sku order by variant_updated_at desc) as sku_rn
	from stg_api_production.spree_variants v
	left join stg_salesforce.product2 a
		on a.sku_variant__c=v.sku
  	left join stg_api_production.variant_stores s
		on s.variant_id=v.id 
	inner join stg_api_production.spree_products p
		on v.product_id=p.id 
	where true
		--and  store_id='1'
		and p.category_id is not null
		and p.deleted_at is null
		and not p.deprecated
		and not v.deprecated
		and v.deleted_at is NULL
	GROUP BY 1,2,3,4,5,6,8,9
	order by variant_updated_at DESC)
,b_idx as (
	select distinct
 		a.f_product_sku_variant__c as variant_sku,
 		a.name as variant_name,
 		v2.id as variant_id,
 		max(v2.id) over (partition by v2.sku) as max_id,
 		v2.product_id,
 		v2.updated_at as variant_updated_at,
 		v.availability_state,
  		v.product_brand,
  		v.article_number,
  		Rank() over (partition by a.f_product_sku_variant__c order by a.createddate desc ) as idx
	from stg_salesforce.asset a
	left join stg_salesforce.product2 p
		on p.sku_variant__c=a.f_product_sku_variant__c
	left join v 
		on a.f_product_sku_variant__c=v.variant_sku
	left join stg_api_production.spree_variants v2
		on v2.sku=a.f_product_sku_variant__c
	where v.variant_sku is null)
, b as (
	select distinct * from b_idx where idx = 1 )
	,f as (
	select
		b.variant_id,
		b.variant_sku,
		b.variant_name,
  		json_extract_path_text(v2.properties, 'color', 'name') as variant_color,
		b.product_id,
		b.variant_updated_at,
		b.availability_state,
  		b.product_brand,
  		b.article_number
	from b
		inner join stg_api_production.spree_variants v2
		on v2.id=b.variant_id
	where variant_id = max_id
	UNION ALL
	select
		variant_id,
		variant_sku,
		variant_name,
  		variant_color,
		product_id,
		variant_updated_at,
		availability_state,
  		product_brand,
  		article_number
	from v
	where sku_rn = 1)
,eans as (
	select 
  	DISTINCT
		variant_id
		,last_value(value ignore nulls) over(partition by variant_id order by created_at ASC rows between unbounded preceding and unbounded following) as ean
	from stg_api_production.eans
)
,upcs as (
	select
  	DISTINCT
		variant_id
		,last_value(value ignore nulls) over(partition by variant_id order by created_at ASC rows between unbounded preceding and unbounded following) as upcs
	from s3_spectrum_rds_dwh_api_production.upcs
	)
select
		f.variant_id,
		f.variant_sku,
		f.variant_name,
  		initcap(case when f.variant_color='#000000' then 'Black'
        	 when f.variant_color='#FFFFFF' or f.variant_color='#ffffff'  then 'White'
            -- when f.variant_color='#AEAEAE' then 'Grey'
        else f.variant_color end) as variant_color,
		f.product_id,
		f.variant_updated_at,
		f.availability_state,
        f.product_brand,
		COALESCE(split_part(ean,'.',1),null) as ean,
		f.article_number,
		upcs
from f
left join eans on eans.variant_id=f.variant_id
left join upcs on upcs.variant_id=f.variant_id
;


BEGIN TRANSACTION;

DELETE FROM bi_ods.variant WHERE 1=1;

INSERT INTO bi_ods.variant
SELECT * FROM tmp_bi_ods_variant;

END TRANSACTION;


