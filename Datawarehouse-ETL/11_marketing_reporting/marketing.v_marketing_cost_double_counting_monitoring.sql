DROP VIEW IF EXISTS marketing.v_marketing_cost_double_counting_monitoring;
CREATE VIEW marketing.v_marketing_cost_double_counting_monitoring AS
SELECT 
  a.date::date AS reporting_date
 ,a.channel
 ,a.account
 ,a.advertising_channel_type
 ,a.campaign_group_name
 ,a.campaign_name
 ,COUNT(*) AS nr_records
FROM marketing.marketing_cost_daily_base_data a 
  LEFT JOIN marketing.marketing_cost_channel_mapping mccm  
    ON a.channel = mccm.channel
    AND CASE 
      WHEN mccm.account IS NOT NULL 
       THEN a.account = mccm.account
      ELSE TRUE
      END
    AND CASE 
      WHEN a.country IS NOT NULL 
       THEN a.country = mccm.country
      ELSE TRUE
      END
    AND CASE 
      WHEN a.customer_type IS NOT NULL 
       THEN a.customer_type = mccm.customer_type
      ELSE TRUE
      END  
    AND CASE 
      WHEN mccm.advertising_channel_type IS NOT NULL 
       THEN a.advertising_channel_type = mccm.advertising_channel_type
      ELSE TRUE
     END
     AND CASE 
    WHEN mccm.campaign_name_contains IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_contains) IN LOWER(a.campaign_name_modified)) > 0
      THEN TRUE
    ELSE FALSE
   END 
  AND CASE 
    WHEN mccm.campaign_name_contains2 IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_contains2) IN LOWER(a.campaign_name_modified)) > 0
      THEN TRUE
    ELSE FALSE
   END 
   AND CASE 
    WHEN mccm.campaign_name_does_not_contain IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_name_does_not_contain) IN LOWER(a.campaign_name_modified)) = 0
      THEN TRUE
    ELSE FALSE
   END
  AND CASE 
    WHEN mccm.campaign_group_name_contains IS NULL
      THEN TRUE
    WHEN POSITION(LOWER(mccm.campaign_group_name_contains) IN LOWER(a.campaign_group_name)) > 0
      THEN TRUE
    ELSE FALSE
   END 
WHERE a.channel NOT IN ('Vouchers', 'Influencers', 'Affiliates', 'Partnerships', 'Organic Search', 'Retail', 'Grover Cash','TV')  
GROUP BY 1,2,3,4,5,6
HAVING nr_records > 1
ORDER BY 1 DESC, 2
WITH NO SCHEMA BINDING;  