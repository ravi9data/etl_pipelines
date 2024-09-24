---Grover Card Reservation---

WITH card_reservation AS (
SELECT  DISTINCT 
    user_id AS customer_id,
    CASE WHEN event_name NOT IN ('atm-withdrawal-declined','payment-failed')  THEN id ELSE NULL END AS payload_id,
    trace_id,
    --NULL AS payload_id,
    CASE WHEN event_name NOT IN ('atm-withdrawal-declined','payment-failed')  THEN card_id ELSE id END AS card_id,
    NULL AS card_scheme,
    merchant_currency AS currency,
    pos_entry_mode,
    --transaction_date::Date,
    event_name,
    merchant_town,
    NULL AS merchant_country_code,
    merchant_name,
    NULL merchant_id,
    NULL AS merchant_category,
    CASE WHEN event_name ='payment-failed' then 'FAILED PURCHASE' ELSE NULL END AS transaction_type,
    NULL AS wallet_type,
    date::timestamp AS event_timestamp,
    reason,
    --amount,
    Replace(SPLIT_PART(amount,',',1),'.','')::decimal(10,2)+
                (TO_NUMBER(SPLIT_PART(SPLIT_PART(amount,',',2),'€',1),'99')/100)::decimal(10,2) AS amount_transaction
    --consumed_at::timestamp AS recorded_at
,wrong_customer AS (---Many customer_id ARE populated BY solaris ID IN kakfa event, hence once time CHANGE IS made 
    SELECT
        DISTINCT card_id,
        min(CASE WHEN len(customer_id)<10 THEN customer_id END)::int AS Act_customer_id,
        min(CASE WHEN len(customer_id)>10 THEN customer_id END) AS mistake_cust
    FROM card_reservation cr
    GROUP BY 1 )
SELECT  ---missing Customer_id's
CASE
    WHEN customer_id ='14f2bbb1-fbba-4897-bfcd-54be19c6b1b8' then '1018904'
    WHEN customer_id ='f21df6bf-a325-4973-bbac-912eab176c45' then '686697'
    WHEN customer_id ='12d4c572-5886-43ef-875d-3438a469f7fa' then '1432785'
    WHEN customer_id ='4de4e80c-872e-431a-bc2b-86ba6eaca4a5' then '1601104'
    WHEN customer_id ='34a03824-57af-4347-9463-7338da849319' then '798534'
    WHEN customer_id ='0559fb01-ca15-4bd3-b175-b6ca7bbc8449' then '444751'
    WHEN customer_id ='10cd014c-7894-4749-bda3-f5961fe7e8d8' then '123894'
    WHEN customer_id ='ec73c368-4afc-4f9a-950a-363b837a72bb' then '193743'
    WHEN customer_id ='343e5bc8-a5af-4a3f-8eac-8ea420e5c0c6' then '274735'
    WHEN wc.mistake_cust IS NOT NULL THEN act_customer_id::text 
    ELSE customer_id::text  
END::int AS customer_id,
    cr.trace_id,
    cr.payload_id,
    --payload_id AS p_id,
    cr.card_id,
    card_scheme,
    currency,
    pos_entry_mode,
    event_name,
    merchant_town,
    merchant_country_code,
    merchant_id,
    merchant_name,
    NULL AS merchant_category_code,
    transaction_type,
    wallet_type,
    event_timestamp,
    event_timestamp::date AS transaction_date,
    reason,
    cr.amount_transaction
    --recorded_at
FROM card_reservation cr 
LEFT JOIN wrong_customer wc 
    ON wc.card_id=cr.card_id    
UNION ALL 
SELECT 
    DISTINCT 
    json_extract_path_text(payload,'user_id')::int AS  customer_id,
    NULL AS trace_id,
    NULL AS payload_id,
    json_extract_path_text(payload,'id') AS card_id,
    NULL AS card_scheme,
    CASE WHEN json_extract_path_text(payload,'amount') LIKE '%€%' THEN 'EUR' ELSE NULL END AS currency,
    NULL AS pos_entry_mode,
    event_name,
    NULL merchant_town,
    NULL AS merchant_country_code,
    NULL merchant_id,
    json_extract_path_text(payload,'merchant_name') AS merchant_name,
    NULL AS merchant_category_code,
    'FAILED PURCHASE' AS transaction_type,
    NULL AS wallet_type,
    json_extract_path_text(payload,'date')::timestamp AS event_timestamp,
    json_extract_path_text(payload,'date')::Date AS transaction_date,
    json_extract_path_text(payload,'reason') AS reason,
    Replace(SPLIT_PART(json_extract_path_text(payload,'amount') ,',',1),'.','')::decimal(10,2)+
                (TO_NUMBER(SPLIT_PART(SPLIT_PART(json_extract_path_text(payload,'amount') ,',',2),'€',1),'99')/100)::decimal(10,2) AS amount_transaction
    --event_timestamp::timestamp AS recorded_at
WHERE event_name ='payment-failed'
and payload LIKE '%359295%'
AND  payload LIKE '%bdcf0625350f288a72c64978beeb7a38mcrd%'
AND  payload LIKE '%PARAM .*TRENDYOL%'
--AND event_timestamp ='2021-06-21 11:46:04.000'
AND  payload LIKE '%704,89%';

