DROP VIEW IF EXISTS dm_debt_collection.v_dpd_assets;
CREATE VIEW dm_debt_collection.v_dpd_assets AS 
SELECT 
    DISTINCT t.asset_id 
    ,t.subscription_id 
    ,t.last_allocation_dpd AS dpd_today
    ,y.last_allocation_dpd AS dpd_yest
    ,(t.last_allocation_dpd - y.last_allocation_dpd) AS dpd_diff
    ,t.date AS date_
FROM master.asset_historical t
    INNER JOIN master.asset_historical y
        ON t.asset_id = y.asset_id
        AND y.date = dateadd(DAY,-1,t.date)
WHERE TRUE
       AND t.date >= dateadd(MONTH, -2,date_trunc('month',current_date))
       AND dpd_diff > 50
WITH NO SCHEMA BINDING
; 

GRANT SELECT ON dm_debt_collection.v_dpd_assets TO tableau;