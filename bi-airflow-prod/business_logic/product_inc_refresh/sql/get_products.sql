-- CTE to get category and subcategory data
WITH category AS (
    SELECT
        pc.id AS category_id,
        pc.name AS category_name,
        s.id AS subcategory_id,
        s.name AS subcategory_name,
        CASE 
            WHEN s.updated_at > pc.updated_at THEN s.updated_at
            ELSE pc.updated_at
        END AS updated_at
    FROM
        api_production.categories s
    LEFT JOIN api_production.categories pc
        ON s.parent_id = pc.id
        AND pc.parent_id IS NULL
    WHERE
        pc.id IS NOT NULL
),

-- CTE to get final product details
p_final AS (
    SELECT DISTINCT
        p.id AS product_id,
        p.sku AS product_sku,
        p.created_at,
        p.updated_at,
        p.name AS product_name,
        c.category_id,
        c.subcategory_id,
        c.category_name,
        c.subcategory_name,
        UPPER(p.brand) AS brand,
        p.slug,
        p.market_price,
        p.rank,
        rl.risk_label,
        rl.risk_category
    FROM
        api_production.spree_products p
    INNER JOIN category c
        ON p.category_id = c.subcategory_id
    LEFT JOIN fraud_and_credit_risk.subcategory_brand_risk_labels rl
        ON LOWER(c.subcategory_name) = rl.subcategory
        AND LOWER(p.brand) = rl.brand
    WHERE
        p.id != 9251
        AND p.updated_at > CURRENT_DATE - INTERVAL '15 days'
)

-- Final selection including checksum
SELECT 
    MD5(CAST(p_final AS TEXT)) AS check_sum,
    *
FROM 
    p_final;
