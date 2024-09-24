DROP VIEW IF EXISTS dm_debt_collection.v_dpd_jumps;
CREATE VIEW dm_debt_collection.v_dpd_jumps AS 
SELECT
    p.asset_id
    ,p.payment_id 
    ,p.subscription_id 
    ,p.status
    ,p1.status AS previous_status
    ,p.dpd
    ,b.dpd_yest
    ,b.dpd_diff
    ,p1.dpd AS dpd_pmyts
    ,p.paid_date
    ,p1.paid_date AS paid_date_ydy
    ,p.next_due_date
    ,p1.next_due_date AS next_due_date_ydy
    ,p.payment_method_details
    ,CASE 
        WHEN p.subscription_id ILIKE 'F-%' 
          THEN 'new_infra' 
        ELSE 'old_infra' 
     END AS infra_status
    ,p.date
    ,CASE 
        WHEN p.paid_date IS NULL AND paid_date_ydy IS NOT NULL 
          THEN 'Possible SEPA Payment Issue'
        WHEN p.paid_date::Date <> paid_date_ydy::date 
          THEN 'Paid Date Changed'
        WHEN p.dpd IN ('19346','19344','19345') 
          THEN 'Known Issue'
        WHEN p.paid_date IS NOT NULL AND paid_date_ydy IS NULL AND previous_status IS NOT NULL 
          THEN 'Paid On Date'
        WHEN p.paid_date IS NOT NULL AND paid_date_ydy IS NULL AND previous_status IS NULL
          THEN 'Paid but Payment is missing on Previous Date'
        /*WHEN p.date = '2023-05-31' 
          THEN 'Glitch on 31/05 in asset_historical Table'*/
        ELSE NULL 
     END  AS dpd_status 
FROM master.subscription_payment_historical p 
    INNER JOIN dm_debt_collection.v_dpd_assets b 
        ON b.asset_id = p.asset_id
        AND b.subscription_id = p.subscription_id 
        AND b.date_ = p.date
        AND b.dpd_today = p.dpd
    LEFT JOIN master.subscription_payment_historical p1
        ON p.payment_id=p1.payment_id
        AND p.date-1=p1.date
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_debt_collection.v_dpd_jumps TO tableau;