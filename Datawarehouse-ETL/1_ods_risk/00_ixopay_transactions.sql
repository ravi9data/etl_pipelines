-- Published payment_methods
DROP TABLE IF EXISTS published_user_payment_methods;
CREATE TEMP TABLE published_user_payment_methods
SORTKEY(merchant_transaction_id)
DISTKEY(merchant_transaction_id)
AS
SELECT
    merchant_transaction_id,
    user_id,
    payment_gateway_id,
    JSON_EXTRACT_PATH_TEXT(meta, 'bank_name') AS card_bank,
    JSON_EXTRACT_PATH_TEXT(meta, 'account_name') AS paypal_name
FROM stg_api_production.user_payment_methods
WHERE status = 'published'
    AND payment_gateway_id IN (1,2,3);

   
DELETE FROM staging.risk_payment_method_events
WHERE modified_at > current_date-2 ;

INSERT INTO staging.risk_payment_method_events
SELECT
    json_extract_path_text(payload,'referenceId') uuid,
    json_extract_path_text(payload,'paymentMethod') payment_method,
    split_part(event_name,'_',1) as "type",
    split_part(event_name,'_',2) as status,
    consumed_at as created_at,
    consumed_at as modified_at,
    json_extract_path_text(payload,'transactionId') merchant_txid,
   	json_extract_path_text(payload,'returnData') return_data,
    CASE
        WHEN return_data IS NOT NULL AND return_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(return_data, 'walletOwner')
        --ELSE pupm.paypal_name
    END AS wallet_owner,
    nullif(json_extract_path_text(payload,'customerData'),'') as customer_data,
    CASE
        WHEN customer_data IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(customer_data, 'firstName')
        ELSE NULL
    END AS first_name,
    CASE
        WHEN customer_data IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(customer_data, 'lastName')
        ELSE NULL
    END AS last_name,
    CASE
        WHEN customer_data IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(customer_data, 'email')
        ELSE NULL
    END AS shopper_email,
    json_extract_path_text(return_data,'creditcardData') creditcard,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'type')
        ELSE NULL
    END AS card_type,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'cardHolder')
        WHEN (return_data IS NOT NULL AND return_data != '[]')
            THEN JSON_EXTRACT_PATH_TEXT(return_data, 'bankAccountOwner')
        ELSE NULL
    END AS card_holder,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binCountry')
        ELSE NULL
    END AS country,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'expiryMonth')
        ELSE NULL
    END AS expiry_month,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'expiryYear')
        ELSE NULL
    END AS expiry_year,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'firstSixDigits')
        ELSE NULL
    END AS first_six_digits,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'lastFourDigits')
        ELSE NULL
    END AS last_four_digits,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'card_region')
        ELSE NULL
    END AS card_region,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binBrand')
        ELSE NULL
    END AS bin_brand,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binCountry')
        WHEN (return_data IS NOT NULL AND return_data != '[]')
            THEN JSON_EXTRACT_PATH_TEXT(return_data, 'bankCountry')
        ELSE NULL
    END AS bin_country,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binType')
        ELSE NULL
    END AS bin_type,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binLevel')
        ELSE NULL
    END AS bin_level,
    CASE
        WHEN customer_data IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(customer_data, 'iban')
        ELSE NULL
    END AS iban,
    CASE
        WHEN customer_data IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(customer_data, 'identification')
        ELSE NULL
    END AS identification,
    CASE
        WHEN creditcard IS NOT NULL AND creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'binBank')
        ELSE NULL
    end AS bank_name
FROM s3_spectrum_kafka_topics_raw.payment_method_events s
WHERE cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND consumed_at > current_date-2;

-- Enriched ixopay transactions
DROP TABLE IF EXISTS enriched_ixopay_transactions;
CREATE TEMP TABLE enriched_ixopay_transactions
AS
SELECT
    it.uuid,
    it.merchant_name,
    it.adapter,
    it."method",
    it."type",
    it.status,
    it.created_at,
    it.modified_at,
    it.initiated_by,
    it.merchant_txid,
    it.related_id,
    it.base_amount,
    it.description,
    it.extra_data,
    CASE
        WHEN it.extra_data IS NOT NULL AND it.extra_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.extra_data, 'storeName')
        ELSE NULL
    END AS store_name,
    CASE
        WHEN it.extra_data IS NOT NULL AND it.extra_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.extra_data, 'storeOffline')
        ELSE NULL
    END AS store_offline,
    CASE
        WHEN it.extra_data IS NOT NULL AND it.extra_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.extra_data, 'orderId')
        ELSE NULL
    END AS order_id,
    CASE
        WHEN it.extra_data IS NOT NULL AND it.extra_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.extra_data, 'scoringDecision')
        ELSE NULL
    END AS scoring_decision,
    CASE
        WHEN it.extra_data IS NOT NULL AND it.extra_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.extra_data, 'customerType')
        ELSE NULL
    END AS customer_type,
    it.transaction_indicator,
    it.registered_at,
    it.return_data,
    CASE
        WHEN it.return_data IS NOT NULL AND it.return_data != '[]'
            THEN JSON_EXTRACT_PATH_TEXT(it.return_data, 'walletOwner')
        ELSE pupm.paypal_name
    END AS wallet_owner,
    it.customer,
    CASE
        WHEN it.customer IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(it.customer, 'first_name')
        ELSE NULL
    END AS first_name,
    CASE
        WHEN it.customer IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(it.customer, 'last_name')
        ELSE NULL
    END AS last_name,
    CASE
        WHEN it.customer IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(it.customer, 'email')
        ELSE NULL
    END AS shopper_email,
    it.creditcard,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'type')
        ELSE NULL
    END AS card_type,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'card_holder')
        WHEN (it.return_data IS NOT NULL AND it.return_data != '[]')
            THEN JSON_EXTRACT_PATH_TEXT(it.return_data, 'bankAccountOwner')
        ELSE NULL
    END AS card_holder,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'country')
        ELSE NULL
    END AS country,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'expiry_month')
        ELSE NULL
    END AS expiry_month,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'expiry_year')
        ELSE NULL
    END AS expiry_year,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'first_six_digits')
        ELSE NULL
    END AS first_six_digits,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(creditcard, 'last_four_digits')
        ELSE NULL
    END AS last_four_digits,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'card_region')
        ELSE NULL
    END AS card_region,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'bin_brand')
        ELSE NULL
    END AS bin_brand,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'bin_country')
        WHEN (it.return_data IS NOT NULL AND it.return_data != '[]')
            THEN JSON_EXTRACT_PATH_TEXT(it.return_data, 'bankCountry')
        ELSE NULL
    END AS bin_country,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'bin_type')
        ELSE NULL
    END AS bin_type,
    CASE
        WHEN it.creditcard IS NOT NULL AND it.creditcard NOT ILIKE '%xxxxxxxxxxx%'
            THEN JSON_EXTRACT_PATH_TEXT(it.creditcard, 'bin_level')
        ELSE NULL
    END AS bin_level,
    CASE
        WHEN it.customer IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(it.customer, 'iban')
        ELSE NULL
    END AS iban,
    CASE
        WHEN it.customer IS NOT NULL
            THEN JSON_EXTRACT_PATH_TEXT(it.customer, 'identification')
        ELSE NULL
    END AS identification,
    pupm.card_bank AS bank_name
FROM stg_external_apis.ixopay_transactions AS it
LEFT JOIN published_user_payment_methods AS pupm ON it.merchant_txid = pupm.merchant_transaction_id;


insert into enriched_ixopay_transactions(uuid,method,"type",status,created_at,modified_at,merchant_txid,wallet_owner,first_name,last_name,shopper_email,creditcard,card_type,card_holder,country,expiry_month,expiry_year,first_six_digits,last_four_digits,card_region,bin_brand,bin_country,bin_type,bin_level,iban,identification,bank_name)
select uuid,payment_method,"type",status,created_at,modified_at,merchant_txid,wallet_owner,first_name,last_name,shopper_email,creditcard,card_type,card_holder,country,expiry_month,expiry_year,first_six_digits,last_four_digits,card_region,bin_brand,bin_country,bin_type,bin_level,iban,identification,bank_name
from staging.risk_payment_method_events
where merchant_txid not in (select merchant_txid from enriched_ixopay_transactions);

DROP TABLE IF EXISTS ods_data_sensitive.ixopay_transactions;
CREATE TABLE ods_data_sensitive.ixopay_transactions AS
SELECT
    CASE
        WHEN eit.type IN ('debit','refund')
            THEN ac.spree_customer_id__c
        WHEN eit.type = 'register'
            THEN identification
    END AS customer_id,
    eit.uuid,
    eit.merchant_name,
    eit.adapter,
    eit.method,
    eit.type,
    eit.status,
    eit.created_at,
    eit.modified_at,
    eit.initiated_by,
    eit.merchant_txid,
    eit.related_id,
    eit.base_amount,
    eit.description,
    eit.extra_data,
    eit.store_name,
    eit.store_offline,
    eit.order_id,
    eit.scoring_decision,
    eit.customer_type,
    eit.transaction_indicator,
    eit.registered_at,
    eit.return_data,
    eit.wallet_owner,
    eit.customer,
    eit.first_name,
    eit.last_name,
    eit.shopper_email,
    eit.creditcard,
    eit.card_type,
    eit.card_holder,
    eit.country,
    eit.expiry_month,
    eit.expiry_year,
    eit.first_six_digits,
    eit.last_four_digits,
    eit.card_region,
    eit.bin_brand,
    eit.bin_country,
    eit.bin_type,
    eit.bin_level,
    eit.iban,
    eit.identification,
    eit.bank_name,
    SPLIT_PART(eit.description, '-', 1) AS order_reference
FROM enriched_ixopay_transactions AS eit
LEFT JOIN stg_salesforce.account AS ac ON (ac.id = eit.identification)
ORDER BY eit.created_at DESC
;
