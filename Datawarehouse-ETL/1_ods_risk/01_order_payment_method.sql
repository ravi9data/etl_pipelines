DROP TABLE IF EXISTS ods_data_sensitive.order_payment_method;

CREATE TABLE ods_data_sensitive.order_payment_method AS
WITH adyen_info AS (
        SELECT
            o.order_id,
            pr.psp_reference AS payment_method_id,
            pr.merchant_reference,
            UPPER(REPLACE(pr.card_type, 'mc', 'mastercard')) AS card_type,
            pr.refusal_reason_raw,
            pr.card_number,
            pr.card_bin,
            CASE
                WHEN pr.card_issuing_country IN ('', 'N/A') THEN NULL
                ELSE pr.card_issuing_country
            END AS card_issuing_country,
            pr.card_issuing_bank,
            pr.funding_source,
            CASE
                WHEN pr.cardholder_name = '' THEN NULL
                ELSE pr.cardholder_name
            END AS cardholder_name,
            CASE
                WHEN pr.shopper_email = '' THEN NULL
                ELSE pr.shopper_email
            END AS shopper_email,
            NULL AS paypal_email,
            'Adyen' AS gateway_type,
            NULL AS iban
            --pr.status_
        FROM ods_data_sensitive.adyen_payment_requests AS pr 
        LEFT JOIN ods_production."order" AS o ON pr.psp_reference = o.payment_method_id_1  
        WHERE pr.amount = 0
          AND o.submitted_date IS NOT NULL
        ),
     ixopay_info AS (
        SELECT
            o.order_id,
            ip.uuid AS payment_method_id,
            ip.merchant_txid AS merchant_reference,
            UPPER(REPLACE(ip.bin_brand, 'mc', 'mastercard')) AS card_type,
            NULL AS refusal_reason_raw,
            ip.last_four_digits AS card_number,
            ip.first_six_digits AS card_bin,
            CASE
                WHEN ip.bin_country IN ('', 'N/A') THEN NULL
                ELSE ip.bin_country
            END AS card_issuing_country,
            CASE
                WHEN bank_name = '' THEN NULL
                ELSE bank_name
            END AS card_issuing_bank,
            CASE
                WHEN ip.bin_type = '' THEN NULL
                ELSE ip.bin_type||' '||ip.bin_level
            END AS funding_source,
            CASE
                WHEN ip.card_holder = '' THEN NULL
                ELSE ip.card_holder
            END AS card_holder_name, 
            ip.shopper_email,
            ip.wallet_owner AS paypal_email, 
            'Ixopay' AS gateway_type,
            CASE
                WHEN ip.iban = '' THEN NULL
                ELSE ip.iban
            END AS iban
        FROM ods_data_sensitive.ixopay_transactions AS ip
        LEFT JOIN ods_production.order AS o ON ip.uuid = o.payment_method_id_2
        WHERE o.submitted_date IS NOT NULL
        ),
     payment_info AS (
        SELECT
            *
        FROM adyen_info 
		UNION ALL
        SELECT
            *
        FROM ixopay_info
        ),
     paypal_records AS (
        SELECT
            user_id,
            reference_id,
            JSON_EXTRACT_PATH_TEXT(meta, 'account_name') AS paypal_name
        FROM stg_api_production.user_payment_methods
        WHERE status = 'published'
          AND payment_gateway_id = 1
        ),
     orders AS (
        SELECT
            so."number" AS order_id,
            sfo.createddate AS submitted_date,
            sfo.payment_method_name__c AS payment_method,
            sfo.payment_method_type__c AS payment_method_type,
            sfo.payment_method_id_1__c AS payment_method_id_1,
            sfo.payment_method_id_2__c AS payment_method_id_2,
            sfo.sepa_mandate_id__c AS sepa_mandate_id,
            sfo.sepa_mandate_date__c AS sepa_mandate_date
        FROM stg_api_production.spree_orders AS so
        LEFT JOIN stg_salesforce."order" AS sfo ON so."number" = sfo.spree_order_number__c
        ),
     tokens AS (
        SELECT
            *,
            RANK() OVER (PARTITION BY token ORDER BY created_at DESC) AS rank_per_token
       FROM stg_api_production.spree_paypal_express_checkouts
       )
SELECT
    o.order_id,
    o.submitted_date,
    o.payment_method,
    o.payment_method_type,
    o.payment_method_id_1,
    o.payment_method_id_2,
    o.sepa_mandate_id,
    o.sepa_mandate_date,
    pi.payment_method_id AS psp_reference,
    pi.merchant_reference,
    pi.card_type,
    COALESCE(pi.paypal_email, pr.paypal_name, t.payer_email) AS paypal_email,
    pi.refusal_reason_raw,
    pi.card_number,
    pi.card_bin,
    pi.card_issuing_country,
    pi.card_issuing_bank,
    pi.funding_source,
    pi.cardholder_name,
    pi.shopper_email,
    pi.gateway_type,
    pi.iban
FROM orders AS o
LEFT JOIN payment_info AS pi ON o.order_id = pi.order_id
LEFT JOIN paypal_records AS pr ON o.payment_method_id_2 = pr.reference_id
LEFT JOIN tokens AS t ON o.payment_method_id_1 = t.token
                     AND t.rank_per_token = 1
WHERE o.submitted_date IS NOT NULL
ORDER BY o.submitted_date DESC
;

GRANT SELECT ON ods_data_sensitive.order_payment_method TO tableau;
