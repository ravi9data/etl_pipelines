DROP TABLE IF EXISTS ods_data_sensitive.customer_pii;

CREATE TABLE ods_data_sensitive.customer_pii AS
WITH web AS (
        SELECT
            DISTINCT user_id AS customer_id,
            MAX(bill_address_id) AS billing_address_id,
            MAX(ship_address_id) AS shipping_address_id
        FROM stg_api_production.spree_orders
        WHERE user_id IS NOT NULL
          AND (bill_address_id IS NOT NULL OR ship_address_id IS NOT NULL)
        GROUP BY user_id
        ), 
     last_address_pre AS (
        SELECT
            user_id,
            MAX(spree_addresses.created_at) AS max_created
        FROM stg_api_production.spree_addresses
        GROUP BY spree_addresses.user_id
        ),
     last_address AS (
        SELECT
            ad.user_id AS customer_id,
            ad.city,
            ad.zipcode,
            ad.address1 AS street,
            ad.address2 AS house_number
        FROM last_address_pre AS la
        JOIN stg_api_production.spree_addresses AS ad ON la.user_id = ad.user_id
                                                     AND ad.created_at = la.max_created
        ),   
     ip_pre AS (
        SELECT
            DISTINCT user_id AS customer_id,
            ip_address,
            creation_time AS last_visited_at
        WHERE user_id NOT IN ('0')
        UNION ALL
        SELECT
            DISTINCT user_id AS customer_id,
            ip_address,
            creation_time AS last_visited_at
        FROM stg_events.registration_view
        WHERE user_id NOT IN ('0')
        ),
     ip AS (
        SELECT
            customer_id,
            LISTAGG(DISTINCT ip_address, ' | ') AS ip_address_list
        FROM ip_pre
        GROUP BY 1
        ),
     paypal_email_agg AS (
        SELECT
            user_id AS customer_id,
            LISTAGG(DISTINCT payer_email, ' / ') AS paypal_email
        FROM stg_api_production.spree_paypal_express_checkouts
        GROUP BY 1
        ),
     schufa_string AS(
        SELECT
            sd.customer_id,
            CASE
                WHEN IS_VALID_JSON(sd.addresses) IS NOT TRUE
                    THEN NULL
                WHEN LENGTH(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address')) != 0
                    THEN INITCAP(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address'), 'Strasse')) || ', ' ||
                         INITCAP(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address'), 'Ort')) || ', ' ||
                         INITCAP(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address'), 'PLZ'))
                WHEN LENGTH(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'Ort')) != 0
                    THEN INITCAP(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'Strasse')) || ', ' ||
                         INITCAP(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'Ort')) || ', ' ||
                         INITCAP(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'PLZ'))
                ELSE NULL
            END AS schufa_address_string,
            CASE
                WHEN IS_VALID_JSON(sd.addresses) IS NOT TRUE
                    THEN NULL
                WHEN LENGTH(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address')) != 0
                    THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'address'), 'PLZ')
                WHEN LENGTH(JSON_EXTRACT_PATH_TEXT(sd.addresses, 'Ort')) != 0
                    THEN JSON_EXTRACT_PATH_TEXT(sd.addresses, 'PLZ')
                ELSE NULL
            END AS schufa_zipcode_string
        FROM (SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY id DESC) AS rn
              FROM stg_realtimebrandenburg.schufa_data) AS sd
        WHERE rn = 1
        ),
     paypal_identity AS (
        SELECT
            customer_id,
            JSON_EXTRACT_PATH_TEXT(op.payment_metadata, 'paypal_first_name') || ' ' || JSON_EXTRACT_PATH_TEXT(op.payment_metadata, 'paypal_last_name') as paypal_name
        FROM (SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY id DESC) AS rn
              FROM s3_spectrum_rds_dwh_order_approval.order_payment AS op
              WHERE op.payment_type = 'paypal'
                AND (JSON_EXTRACT_PATH_TEXT(op.payment_metadata, 'paypal_first_name') != ''
                     OR JSON_EXTRACT_PATH_TEXT(op.payment_metadata, 'paypal_last_name') != ''
                    )
             ) AS op
        WHERE rn = 1
        )
SELECT
    DISTINCT u.id AS customer_id,
    comp.id AS company_id,
    u.created_at,
    u.updated_at,
    CASE
        WHEN LOWER(u.gender) IN ('f','w') THEN 'f'
        WHEN LOWER(u.gender) IN ('m') THEN 'm'
        ELSE NULL
    END AS gender,
    "LEFT"('NOW'::TEXT::DATE::CHARACTER VARYING::TEXT, 4)::NUMERIC - "LEFT"(u.birthdate::CHARACTER VARYING::TEXT, 4)::NUMERIC AS age,
    u.subscription_limit,
    u.first_name,
    u.last_name,
    u.birthdate,
    u.email,
    u.phone_number,
    sp.paypal_email,
    u.subscription_limit_change_date,	
    COALESCE(u.user_type, 'normal_customer') AS customer_type,
    comp.name AS company_name,
    comp.ust AS tax_id,
    comp.hrv AS handelsregister,
    CASE
        WHEN u.mailchimp_status = 'unsubscribed'
            THEN 'Unsubscribed'
        WHEN u.mailchimp_status = 'subscribed'
         AND u.confirmed_at IS NULL
            THEN 'Subscribed'
        WHEN u.mailchimp_status = 'subscribed'
         AND u.confirmed_at IS NOT NULL
            THEN 'Opted In'
    END AS email_subscribe,
    COALESCE(cs.income_range, 'N/A') AS income_range,
    COALESCE(cs.education_level, 'N/A') AS education_level,
    c1.name AS billing_country,
    TRIM(LOWER(a1.city)) AS billing_city,
    a1.zipcode AS billing_zip,
    c2.name AS shipping_country,
    COALESCE(TRIM(LOWER(a2.city)), TRIM(LOWER(la.city)))::CHARACTER VARYING(128) AS shipping_city,
    COALESCE(a2.zipcode, la.zipcode)::CHARACTER VARYING(16) AS shipping_zip,
    a1.address1 AS street,
    a1.address2 AS house_number,
    u.signup_language,
    u.default_locale,
    ip_address_list,
    CASE
        ELSE FALSE
    CASE
        WHEN g.employee_email IS NULL THEN FALSE
        ELSE TRUE
    CASE
        WHEN COALESCE(a2.zipcode, la.zipcode)::CHARACTER VARYING(16) = '10179'
         AND COALESCE(TRIM(LOWER(a2.address1)), TRIM(LOWER(la.street)))::CHARACTER VARYING(128) ILIKE '%holzmarktstr%'
         AND COALESCE(a2.address2, la.house_number)::CHARACTER VARYING(16) = '11'
            THEN TRUE
        ELSE FALSE
    pi.paypal_name,
    ss.schufa_address_string,
    ss.schufa_zipcode_string
FROM stg_api_production.spree_users AS u
LEFT JOIN stg_api_production.companies AS comp ON u.company_id = comp.id
LEFT JOIN web AS w ON u.id = w.customer_id
LEFT JOIN last_address AS la ON u.id = la.customer_id
LEFT JOIN stg_api_production.spree_addresses AS a1 ON a1.id = COALESCE(w.billing_address_id, u.bill_address_id)
LEFT JOIN stg_api_production.spree_addresses AS a2 ON a2.id = COALESCE(w.shipping_address_id, u.ship_address_id)
LEFT JOIN stg_api_production.spree_countries AS c1 ON a1.country_id = c1.id
LEFT JOIN stg_api_production.spree_countries AS c2 ON a2.country_id = c2.id
LEFT JOIN paypal_email_agg AS sp ON u.id = sp.customer_id
LEFT JOIN ip ON u.id = ip.customer_id
LEFT JOIN ods_data_sensitive.customer_survey AS cs ON u.id = cs.customer_id
LEFT JOIN schufa_string AS ss ON u.id = ss.customer_id
LEFT JOIN paypal_identity AS pi ON u.id = pi.customer_id
WHERE u.deleted_at IS NULL
;

GRANT SELECT ON ods_data_sensitive.customer_pii TO tableau;
GRANT SELECT ON ods_data_sensitive.customer_pii TO basri_oz;
