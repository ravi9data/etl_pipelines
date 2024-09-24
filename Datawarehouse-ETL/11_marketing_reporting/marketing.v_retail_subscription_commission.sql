DROP VIEW IF EXISTS marketing.v_retail_subscription_commisson;
CREATE VIEW marketing.v_retail_subscription_commisson AS
WITH stores_ AS (
    SELECT
        distinct
        datum as fact_date,
        s.id,
        s.store_type,
        s.country_name
    from public.dim_dates dd
             inner join ods_production.store s
                        on datum >= s.created_date::date
    WHERE datum BETWEEN '2022-09-01' AND current_date
      AND s.account_name IN ('Media Markt', 'Saturn')
      AND s.country_name IN ('Germany', 'Austria', 'Spain')
)
   ,subscriptions AS (
    SELECT distinct
        ss.fact_date,
        ss.country_name,
        s.start_date,
        s.subscription_id,
        s.status,
        s.allocation_status,
        DATEADD('day',(CASE WHEN s.store_type ='online' THEN 14 ELSE 2 END),first_asset_delivery_date::TIMESTAMP) as completed_date,
        --DATES
        s.first_asset_delivery_date,
        s.last_return_shipment_at,
        CASE
            WHEN s.start_date::DATE < '2023-09-01' AND a.widerruf_claim_date IS NOT NULL THEN 'Excluded'
            WHEN s.start_date::DATE >= '2023-09-01' AND s.is_widerruf = 'true' THEN 'Excluded'
            WHEN s.store_type = 'online' AND  DATEDIFF('day', s.first_asset_delivery_date::TIMESTAMP, last_return_shipment_at::TIMESTAMP) <= 14 THEN 'Excluded'
            WHEN s.store_type = 'offline' AND  DATEDIFF('day', s.first_asset_delivery_date::TIMESTAMP, last_return_shipment_at::TIMESTAMP) <= 2 THEN 'Excluded'
            WHEN s.store_type = 'online' AND  DATEDIFF('day', s.first_asset_delivery_date::TIMESTAMP, cancellation_date::TIMESTAMP) <= 14 THEN 'Excluded'
            WHEN s.store_type = 'offline' AND  DATEDIFF('day', s.first_asset_delivery_date::TIMESTAMP, cancellation_date::TIMESTAMP) <= 2 THEN 'Excluded'
            WHEN s.allocation_status = 'PENDING ALLOCATION' then 'Excluded'
            WHEN s.store_type IS NOT NULL AND s.first_asset_delivery_date IS NULL THEN 'Excluded'
            WHEN s.store_type IS NULL THEN NULL
            ELSE 'Included' END AS logic_
    FROM stores_ ss
             LEFT JOIN master.subscription s
                       ON s.start_date::date = ss.fact_date
                           AND s.store_id = ss.id
             LEFT JOIN ods_production.allocation a
                       on a.subscription_id = s.subscription_id
    WHERE s.account_name IN ('Media Markt', 'Saturn')
      AND CASE WHEN ss.country_name = 'Spain' THEN s.start_date::DATE >= '2023-09-01'
         ELSE s.start_date::DATE >= '2022-09-01' END
    ORDER BY 1, 5 ASC)

SELECT DISTINCT completed_date, 
    subscription_id, 
    country_name, 
    start_date, 
    CASE WHEN country_name = 'Germany' 
        AND DATE_TRUNC('month',start_date) IN ('2023-11-01','2024-03-01') THEN 70
        ELSE 60 END AS cost
FROM subscriptions
WHERE logic_ = 'Included' AND completed_date <= CURRENT_DATE
WITH NO SCHEMA BINDING;
