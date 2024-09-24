DELETE FROM dm_finance.asset_collection_curves_aggregated
WHERE reporting_date < DATE_ADD('MONTH', -6, DATE_TRUNC('MONTH', CURRENT_DATE))
  OR reporting_date = DATE_ADD('MONTH', -1, DATE_TRUNC('MONTH', CURRENT_DATE))
;

INSERT INTO dm_finance.asset_collection_curves_aggregated
/*WE USE THIS CTE TO REPORT ON ASSETS LIFETIME
 * OF ASSETS THAT HAVE BEEN WITH M&G AT SOME POINT*/
WITH m_and_g_assets AS (
SELECT DISTINCT asset_id 
FROM dm_finance.asset_collection_curves_historical
WHERE capital_source_name = 'SUSTAINABLE TECH RENTAL EUROPE II GMBH'
)
,fasanara_assets AS (
SELECT DISTINCT h.asset_id 
FROM dm_finance.asset_collection_curves_historical h
  LEFT JOIN m_and_g_assets m
    ON h.asset_id = m.asset_id
WHERE TRUE 
  AND h.capital_source_name IN ('FinCo I', 'Grover Finance I GmbH', 'Grover Finance II GmbH')
  AND m.asset_id IS NULL  
)
,country_revenue AS (
/*WE USE THIS AND NEXT CTE TO ALLOCATE A SINGLE COUNTRY
 * TO EACH ASSET (MOST REVENUE GENERATING)*/
SELECT 
 asset_id 
,reporting_date 
,NULLIF(country, '') AS country 
,SUM(net_amount_paid - chargeback_paid - refunds_paid) AS gross_amount_paid 
FROM dm_finance.asset_collection_curves_historical
WHERE reporting_date  = DATE_ADD('MONTH', -1, DATE_TRUNC('MONTH', CURRENT_DATE))
GROUP BY 1,2,3  
)
,most_revenue_generating_country AS (
SELECT DISTINCT
  asset_id 
 ,reporting_date 
 ,country AS country_manipulated
FROM country_revenue
WHERE TRUE
  AND country IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY asset_id, reporting_date  ORDER BY gross_amount_paid DESC) = 1
)
SELECT 
  ac.reporting_date 
 ,ac.cohort_month 
 ,ac.shifted_creation_cohort 
 ,ac.months_on_book 
 ,ac.months_since_shifted_creation_cohort
 ,ac.category_name 
 ,NULLIF(ac.country,'') AS country 
 ,rc.country_manipulated
 ,ac.capital_source_name
 ,CASE
 	 WHEN m.asset_id IS NOT NULL 
 	  THEN TRUE 
 	 ELSE FALSE
 END asset_was_with_m_and_g
 ,CASE
 	 WHEN f.asset_id IS NOT NULL 
 	  THEN TRUE 
 	 ELSE FALSE
 END asset_was_with_fasanara 
 ,SUM(ac.net_amount_paid) AS net_amount_paid
 ,NULLIF(COUNT(DISTINCT CASE
 	 WHEN ac.months_on_book = 0 
 	  THEN ac.asset_id
  END), 0) AS purchase_cohort_nr_assets
 ,NULLIF(COUNT(DISTINCT CASE
 	 WHEN ac.months_since_shifted_creation_cohort = 0 
 	   AND ac.reporting_date >= ac.shifted_creation_cohort 
 	  THEN ac.asset_id
  END), 0) AS shifted_creation_cohort_nr_assets
 ,SUM(CASE
 	 WHEN ac.months_on_book = 0 
 	  THEN ac.initial_price
  END) AS purchase_cohort_total_asset_investment
 ,SUM(CASE
 	 WHEN ac.months_since_shifted_creation_cohort = 0 
 	   AND ac.reporting_date >= ac.shifted_creation_cohort 
 	  THEN ac.initial_price
  END) AS shifted_creation_cohort_total_asset_investment
FROM dm_finance.asset_collection_curves_historical ac
  LEFT JOIN m_and_g_assets m
    ON ac.asset_id = m.asset_id
  LEFT JOIN fasanara_assets f
    ON ac.asset_id = f.asset_id    
  LEFT JOIN most_revenue_generating_country rc
    ON ac.asset_id = rc.asset_id 
   AND ac.reporting_date = rc.reporting_date  
WHERE ac.reporting_date = DATE_ADD('DAY', -1, DATE_TRUNC('MONTH', CURRENT_DATE))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;