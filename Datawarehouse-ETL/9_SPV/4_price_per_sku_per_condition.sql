delete from ods_spv_historical.price_per_condition where reporting_date=(current_Date-1);

insert into ods_spv_historical.price_per_condition
WITH a AS (
         SELECT DISTINCT b.product_sku,
            max(
                CASE
                    WHEN b.asset_condition = 'Neu'::text THEN b.final_market_price
                    ELSE NULL
                END) AS neu_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Wie neu'::text THEN b.final_market_price
                    ELSE NULL
                END) AS as_good_as_new_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Sehr gut'::text THEN b.final_market_price
                    ELSE NULL
                END) AS sehr_gut_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Gut'::text THEN b.final_market_price
                    ELSE NULL
                END) AS gut_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Akzeptabel'::text THEN b.final_market_price
                    ELSE NULL
                END) AS akzeptabel_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Neu'::text THEN b.avg_price
                    ELSE NULL
                END) AS neu_price_before_discount,
            max(
                CASE
                    WHEN b.asset_condition = 'Wie neu'::text THEN b.avg_price
                    ELSE NULL
                END) AS as_good_as_new_price_before_discount,
            max(
                CASE
                    WHEN b.asset_condition = 'Sehr gut'::text THEN b.avg_price
                    ELSE NULL
                END) AS sehr_gut_price_before_discount,
            max(
                CASE
                    WHEN b.asset_condition = 'Gut'::text THEN b.avg_price
                    ELSE NULL
                END) AS gut_price_before_discount,
            max(
                CASE
                    WHEN b.asset_condition = 'Akzeptabel'::text THEN b.avg_price
                    ELSE NULL
                END) AS akzeptabel_price_before_discount,
            max(
                CASE
                    WHEN b.asset_condition = 'Neu'::text THEN b.months_since_last_price
                    ELSE NULL
                END) AS m_since_neu_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Wie neu'::text THEN b.months_since_last_price
                    ELSE NULL
                END) AS m_since_as_good_as_new_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Sehr gut'::text THEN b.months_since_last_price
                    ELSE NULL
                END) AS m_since_sehr_gut_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Gut'::text THEN b.months_since_last_price
                    ELSE NULL
                END) AS m_since_gut_price,
            max(
                CASE
                    WHEN b.asset_condition = 'Akzeptabel'::text THEN b.months_since_last_price
                    ELSE NULL
                END) AS m_since_akzeptabel_price
           FROM ods_spv_historical.spv_used_asset_price_master b
  		  where b.reporting_date=(current_Date-1)
          GROUP BY b.product_sku
        )
         SELECT 
         	current_date::date-1 as reporting_date,
            a.product_sku,
            a.neu_price,
            a.as_good_as_new_price,
            a.sehr_gut_price,
            a.gut_price,
            a.akzeptabel_price,
            a.neu_price_before_discount,
            a.as_good_as_new_price_before_discount,
            a.sehr_gut_price_before_discount,
            a.gut_price_before_discount,
            a.akzeptabel_price_before_discount,
            a.m_since_neu_price,
            a.m_since_as_good_as_new_price,
            a.m_since_sehr_gut_price,
            a.m_since_gut_price,
            a.m_since_akzeptabel_price,
            a.neu_price AS new_price_standardized,
            COALESCE(a.as_good_as_new_price, a.sehr_gut_price) AS agan_price_standardized,
            COALESCE(a.as_good_as_new_price_before_discount, a.sehr_gut_price_before_discount) AS agan_price_standardized_before_discount,
            COALESCE(a.m_since_as_good_as_new_price, a.m_since_sehr_gut_price) AS m_since_agan_price_standardized,
                CASE
                    WHEN (COALESCE(a.sehr_gut_price, 0) + COALESCE(a.gut_price, 0) + COALESCE(a.akzeptabel_price, 0)) = 0 THEN NULL
                    ELSE (COALESCE(a.sehr_gut_price, 0) + COALESCE(a.gut_price, 0) + COALESCE(a.akzeptabel_price, 0)) / (COALESCE(a.sehr_gut_price - a.sehr_gut_price + 1, 0) + COALESCE(a.gut_price - a.gut_price + 1, 0) + COALESCE(a.akzeptabel_price - a.akzeptabel_price + 1, 0))
                END AS used_price_standardized,
                CASE
                    WHEN (COALESCE(a.sehr_gut_price_before_discount, 0) + COALESCE(a.gut_price_before_discount, 0) + COALESCE(a.akzeptabel_price_before_discount, 0)) = 0 THEN NULL
                    ELSE (COALESCE(a.sehr_gut_price_before_discount, 0) + COALESCE(a.gut_price_before_discount, 0) + COALESCE(a.akzeptabel_price_before_discount, 0)) / (COALESCE(a.sehr_gut_price_before_discount - a.sehr_gut_price_before_discount + 1, 0) + COALESCE(a.gut_price_before_discount - a.gut_price_before_discount + 1, 0) + COALESCE(a.akzeptabel_price_before_discount - a.akzeptabel_price_before_discount + 1, 0))
                END AS used_price_standardized_before_discount,
            LEAST(a.m_since_sehr_gut_price, a.m_since_gut_price, a.m_since_akzeptabel_price) AS m_since_used_price_standardized
           FROM a