-- Drop the table if it already exists
DROP TABLE IF EXISTS dm_operations.product_details_and_sourcing;

-- Create the table with the desired data
CREATE TABLE dm_operations.product_details_and_sourcing AS 

-- Last purchase order date for each product SKU
WITH purchase_order_last AS (
    SELECT  
        product_sku,
        MAX(purchase_item_created_date) AS last_purchase_order_date 
    FROM 
        ods_production.purchase_request_item pri 
    GROUP BY 
        1
),

-- Active online products in the DE store with pricing information
pricing AS (
    SELECT DISTINCT 
        product_sku,
        TRUE AS is_active_online
    FROM 
    WHERE 
        store_parent = 'de'
), 

--List of variants with multiple EANs (Bart's request)
eans_per_variant AS (
	SELECT 
		"sku variant" AS variant_sku,
		CASE 
			WHEN "ean 4" IS NOT NULL
				THEN "ean 1"|| '|' || COALESCE("ean 2",'')|| '|' || COALESCE("ean 3",'')|| '|' || COALESCE("ean 4",'')|| '|' || COALESCE("ean 5",'')
			WHEN "ean 4" IS NOT NULL
				THEN "ean 1"|| '|' || COALESCE("ean 2",'')|| '|' || COALESCE("ean 3",'')|| '|' || COALESCE("ean 4",'')
			WHEN "ean 3" IS NOT NULL 
				THEN "ean 1"|| '|' || COALESCE("ean 2",'')|| '|' || COALESCE("ean 3",'')	
			WHEN "ean 2" IS NOT NULL 
				THEN "ean 1"|| '|' || COALESCE("ean 2",'')
			ELSE "ean 1"	
		END AS eans_list
	FROM staging_google_sheet.wemalo_softeon_sku_ean_list
)
SELECT 
    v.variant_sku,
    COALESCE(pv.eans_list,v.ean,v.upcs) AS eans_upcs_list,
    NULLIF(v.variant_color, '') AS variant_color,
    p.product_sku,
    p.product_name,
    p.category_name,
    p.subcategory_name,
    p.brand, 
    p.created_at AS sku_date_of_creation,
    p.updated_at AS sku_last_updated_date,
    l.last_purchase_order_date,

    -- Determining if the SKU is actively sourced
    CASE 
        WHEN DATEDIFF('month', l.last_purchase_order_date, CURRENT_DATE) <= 6 
            OR DATEDIFF('month', p.created_at, CURRENT_DATE) <= 12
        THEN TRUE 
        ELSE FALSE
    END AS is_sku_actively_sourced,

    -- Determining if the SKU is active in the DE store
    COALESCE(
        CASE 
            WHEN p.product_name LIKE '%(Inactive)%' THEN FALSE 
            ELSE pr.is_active_online 
        END, 
        FALSE
    ) AS is_active_de_store,

    -- Identifying if the product is for the US or EU market
    CASE 
        WHEN p.product_name LIKE '%(US)%' THEN 'US' 
        ELSE 'EU' 
    END AS eu_us,

    -- Checking if the product is deprecated
    CASE 
        WHEN p.product_name LIKE '%(Inactive)%' THEN TRUE 
        ELSE FALSE 
    END AS is_deprecated,

    pf.model_name,
    pf.model_family,

    -- Identifying product network technology (LTE/4G/5G)
    CASE 
        WHEN p.product_name LIKE '%LTE%' THEN 'LTE'
        WHEN p.product_name LIKE '%- 4G -%' THEN '4G'
        WHEN p.product_name LIKE '%- 5G -%' THEN '5G'    
    END AS LTE_4G_5G,

    -- Extracting structured specifications
    NULLIF(json_extract_path_text(structured_specifications, 'screen_size'), '') AS screen_size,
    NULLIF(json_extract_path_text(structured_specifications, 'storage_capacity'), '') AS storage_capacity,
    NULLIF(json_extract_path_text(structured_specifications, 'ram'), '') AS ram

FROM 
    ods_production.variant v
LEFT JOIN 
    ods_production.product p ON v.product_id = p.product_id  
LEFT JOIN 
    purchase_order_last l ON l.product_sku = p.product_sku 
LEFT JOIN 
    pricing pr ON pr.product_sku = p.product_sku 
LEFT JOIN 
    pricing.mv_product_family_check pf ON pf.product_sku = p.product_sku    
LEFT JOIN 
    stg_api_production.spree_products sp ON sp.id = p.product_id
LEFT JOIN 
	eans_per_variant pv ON pv.variant_sku = v.variant_sku; 

-- Granting SELECT permissions
GRANT SELECT ON dm_operations.product_details_and_sourcing TO bart;
