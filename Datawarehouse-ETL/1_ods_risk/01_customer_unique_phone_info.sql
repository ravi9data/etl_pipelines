DROP TABLE IF EXISTS ods_production.customer_unique_phone_info;

CREATE TABLE ods_production.customer_unique_phone_info AS
WITH linked_customers AS (
        SELECT
            fl.existing_customer_id AS main_customer_id,
            fl.new_customer_id AS related_customer_id,
            fl.*,
            u1.created_at AS new_created_at,
            u1.email AS new_email,
            u1.phone_number AS new_phone_number,
            u1.birthdate AS new_birthdate,
            u1.first_name || '  '  || u1.last_name AS new_full_name
        FROM stg_detectingfrankfurt.fraud_links AS fl
        LEFT JOIN stg_api_production.spree_users AS u1 ON fl.new_customer_id = u1.id
        WHERE fl.fraud_type = 'NON_UNIQUE_PHONE_NUMBER'
        ),
     linked_customers_2 AS (
        SELECT
            fl.new_customer_id AS main_customer_id,
            fl.existing_customer_id AS related_customer_id,
            fl.*,
            u1.created_at AS new_created_at,
            u1.email AS new_email,
            u1.phone_number AS new_phone_number,
            u1.birthdate AS new_birthdate,
            u1.first_name || '  '  || u1.last_name AS new_full_name
        FROM stg_detectingfrankfurt.fraud_links AS fl
        LEFT JOIN stg_api_production.spree_users AS u1 ON fl.existing_customer_id = u1.id
        WHERE fl.fraud_type = 'NON_UNIQUE_PHONE_NUMBER'
        ),
     grouped AS (
        SELECT
            main_customer_id,
            '{"related_customer_id": "' || related_customer_id || '", "related_created_at": "' || new_created_at ||
            '", "related_email": "' || new_email || '", "related_phone_number": "' || COALESCE(new_phone_number, 'NULL') ||
            '", "related_full_name": "' || new_full_name || '", "last_order_status": "' || COALESCE(o1.status, 'NULL') || '"}' AS link_info
        FROM linked_customers AS lc1
        LEFT JOIN ods_production."order" AS o1 ON lc1.related_customer_id = o1.customer_id
                                              AND o1.order_rank = o1.total_orders
        UNION ALL
        SELECT
            main_customer_id,
            '{"related_customer_id": "' || related_customer_id || '", "related_created_at": "' || new_created_at ||
            '", "related_email": "' || new_email || '", "related_phone_number": "' || COALESCE(new_phone_number, 'NULL') ||
            '", "related_full_name": "' || new_full_name || '", "last_order_status": "' || COALESCE(o2.status, 'NULL') || '"}' AS link_info
        FROM linked_customers_2 AS lc2
        LEFT JOIN ods_production."order" AS o2 ON lc2.related_customer_id = o2.customer_id
                                              AND o2.order_rank = o2.total_orders
        ),
	limit_ AS (
        SELECT
            main_customer_id,
            COUNT(*) AS total_records
        FROM grouped
        GROUP BY 1
        HAVING total_records < 100
        ) 
SELECT
    g.main_customer_id,
    '[' || LISTAGG(link_info, ', ') WITHIN GROUP (ORDER BY g.main_customer_id) || ']' AS related_customer_info
FROM limit_ AS l
INNER JOIN grouped AS g ON l.main_customer_id = g.main_customer_id
GROUP BY 1
;

GRANT SELECT ON ods_production.customer_unique_phone_info TO tableau;
