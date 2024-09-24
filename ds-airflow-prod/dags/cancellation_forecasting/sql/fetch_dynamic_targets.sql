SELECT 
    date AS start_date, 
    subcategory AS subcategory_name, 
    kpi AS index,
    value,
    case when store_final IN ('B2B/Freelancers','B2B/Non-Freelancers') then 'B2B' else store_final end as customer_type
FROM 
     dm_commercial.v_target_dynamic_data_cancellation_rate_forecast
WHERE 
    country = '{country}'
    AND customer_type = '{customer_type}'