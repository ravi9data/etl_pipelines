DROP TABLE IF EXISTS dm_debt_collection.insolvent_cases_nl;
CREATE TABLE dm_debt_collection.insolvent_cases_nl AS
WITH base AS (
    SELECT 
        nl.agent,
        nl.first_name,
        nl.last_name,
        nl.birthdate,
        nl.customer_id,
        nl.subscription_name,
        nl.product,
        nl.returned,
        nl.external_dca,
        nl.first_contact_date,
        nl.last_contact_date,
        nl.contact_method,
        nl.insolvency_date,
        nl.agency,
        nl.contact_person_name,
        nl.agency_phone_number,
        nl.agency_email,
        nl.outstanding_subscription_amount,
        nl.market_value,
        nl.promised_date,
        nl.promised_amount,
        nl.amount_paid,
        nl.final_agreed_amount,
        nl.comment
    FROM staging.insolvent_cases_nl nl
)
, customer_details AS (
    SELECT 
        b.customer_id,
        INITCAP(cp.first_name) AS first_name,
        INITCAP(cp.last_name) AS last_name, 
        cp.birthdate
    FROM base b
    LEFT JOIN ods_data_sensitive.customer_pii cp 
        ON b.customer_id = cp.customer_id
)
, product_details AS (
    SELECT 
        s.subscription_id,
        b.subscription_name,
        s.product_name,
        a.asset_id,
        a.allocation_status_original,
        a2.serial_number,
        a2.residual_value_market_price AS current_market_value,
        a2.amount_rrp
    FROM base b
    LEFT JOIN master.subscription s
        ON b.subscription_name = COALESCE(s.subscription_sf_id, s.subscription_id) 
    LEFT JOIN master.allocation a
        ON s.subscription_id = a.subscription_id
    LEFT JOIN master.asset a2
        ON a.asset_id = a2.asset_id
    WHERE a.created_at = (
        SELECT MAX(a2.created_at)
        FROM master.allocation a2
        WHERE a2.subscription_id = a.subscription_id
    )
)
, outstanding_amount AS (
    SELECT 
        s.subscription_id,
        b.subscription_name,
        sc.outstanding_subscription_revenue
    FROM base b
    LEFT JOIN master.subscription s
        ON b.subscription_name = COALESCE(s.subscription_sf_id, s.subscription_id)
    LEFT JOIN ods_production.subscription_cashflow sc
        ON s.subscription_id = sc.subscription_id
)
SELECT DISTINCT   
    b.agent, 
    b.customer_id,
    cd.first_name,
    cd.last_name, 
    cd.birthdate,
    b.insolvency_date,
    b.first_contact_date,
    b.last_contact_date,
    b.contact_method,
    b.external_dca,
    b.agency,
    b.contact_person_name,
    b.agency_phone_number,
    b.agency_email,
    pd.subscription_name,
    pd.product_name,
    pd.serial_number,
    b.returned,
    b.comment,
    b.promised_date,
    o.outstanding_subscription_revenue,
    pd.current_market_value,
    pd.amount_rrp,
    b.promised_amount,
    b.final_agreed_amount,
    b.amount_paid
FROM base b
LEFT JOIN customer_details cd
    ON b.customer_id = cd.customer_id 
LEFT JOIN product_details pd
    ON b.subscription_name = pd.subscription_name
LEFT JOIN outstanding_amount o
    ON b.subscription_name = o.subscription_name
ORDER BY cd.first_name;
