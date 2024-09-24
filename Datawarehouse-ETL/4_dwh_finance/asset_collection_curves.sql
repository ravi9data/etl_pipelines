-----------------------------PAYMENT COUNTRY-------------------------------------------------
DELETE FROM trans_dev.asset_payment_country WHERE date=date_trunc('month',current_date);

DROP TABLE IF EXISTS payment_country;

CREATE TEMP TABLE payment_country AS
WITH payment_country AS (
SELECT DISTINCT 
   sph.date
  ,sph.asset_id
  ,sph.country_name
  ,COALESCE(sph.paid_date,sph.due_date) AS date_
  ,sph.subscription_start_date AS start_date
FROM master.subscription_payment_historical sph
WHERE TRUE
  AND sph.country_name IS NOT NULL
  AND sph.asset_was_returned IS FALSE 
  AND sph.paid_date IS NOT NULL 
  AND sph.date=date_trunc('month',current_date)
UNION
SELECT
   pa.date
  ,pa.asset_id
  ,s.country_name
  ,COALESCE(pa.paid_date,pa.due_date) AS date_
  ,s.start_date
FROM master.asset_payment_historical pa
  LEFT JOIN master.subscription s 
    ON pa.subscription_id = s.subscription_id
WHERE s.country_name IS NOT NULL
 AND pa.date=date_trunc('month',current_date) 
UNION
SELECT
   rp.date
  ,rp.asset_id
  ,s.country_name
  ,rp.paid_date AS date_
  ,s.start_date 
FROM master.refund_payment_historical rp
  LEFT JOIN master.subscription_payment_historical ps 
    ON ps.payment_id = rp.subscription_payment_id
   AND ps."date" = rp."date"
  LEFT JOIN master.subscription s 
    ON rp.subscription_id = s.subscription_id
WHERE TRUE 
  AND rp.date::DATE = DATE_TRUNC('MONTH',rp.date)::DATE
  AND s.country_name IS NOT NULL
  AND ps.asset_was_returned IS FALSE
  and rp.date=date_trunc('month',current_date) 
)
,prep AS (
SELECT 
*,
RANK() OVER (PARTITION BY asset_id,date ORDER BY date_ DESC,start_date DESC) AS payment_rank 
FROM payment_country
)
SELECT 
date,
asset_id ,
country_name 
FROM prep 
WHERE payment_rank = 1;

INSERT INTO trans_dev.asset_payment_country 
SELECT * FROM payment_country;

DROP TABLE IF EXISTS payment_country; 

-----------------------------SUBSCRIPTION COUNTRY-------------------------------------------------

DELETE FROM trans_dev.asset_subscription_country WHERE datum_month=date_trunc('month',current_date);

DROP TABLE IF EXISTS asset_subscription_country;  
CREATE TEMP TABLE asset_subscription_country AS 
WITH asset_alloc_rank AS (
SELECT DISTINCT
   DATE_TRUNC('MONTH',ah.date)::DATE AS snp_date
  ,ah.asset_id
  ,s.country_name
  ,RANK() OVER (PARTITION BY ah.asset_id,snp_date ORDER BY a.allocated_at DESC) AS alloc_rank 
FROM master.asset_historical ah
  LEFT JOIN master.allocation_historical a 
	  ON a.asset_id = ah.asset_id 
   AND a.date = ah.date
  LEFT JOIN master.subscription s 
    ON a.subscription_id = s.subscription_id 
WHERE TRUE 
  AND a.date=date_trunc('month',current_date)
  AND s.country_name IS NOT NULL
  AND a.delivered_at IS NOT NULL 
  AND a.allocation_status_original <> 'TO BE FIXED'
)
SELECT 
  snp_date AS datum_month
 ,asset_id
 ,country_name
FROM asset_alloc_rank 
WHERE alloc_rank = 1
;


INSERT INTO trans_dev.asset_subscription_country 
SELECT * FROM asset_subscription_country;

DROP TABLE IF EXISTS asset_subscription_country; 

-----------------------------ASSET COHORT MONTH-------------------------------------------------
DROP TABLE IF EXISTS trans_dev.asset_cohort_month;
CREATE TABLE trans_dev.asset_cohort_month AS
/*#############################################
Generate Vector of Dates for each Asset
#############################################*/
WITH payment_base AS (
SELECT 
  asset_id 
 ,DATE_TRUNC('month',MIN(subscription_start_date))::DATE AS first_rent_month
FROM master.subscription_payment sp  
GROUP BY 1
)
SELECT 
  a.asset_id
 ,a.purchased_date::DATE AS cohort
 ,DATE_TRUNC('MONTH',a.purchased_date)::DATE AS cohort_month
 ,GREATEST(LEAST(DATEADD(MONTH, 6, DATE_TRUNC('month', a.created_date)::DATE), COALESCE(pb.first_rent_month, '9999-12-31'))
           , cohort_month)::DATE shifted_creation_cohort  
 ,d.datum
 ,DATE_TRUNC('MONTH',d.datum)::DATE AS datum_month
FROM ods_production.asset a
  CROSS JOIN public.dim_dates d
	LEFT JOIN payment_base pb 
      ON a.asset_id = pb.asset_id  
WHERE TRUE 
 AND a.purchased_date <= d.datum 
 AND d.datum <= COALESCE(sold_date,lost_date,
   CASE 
  	WHEN asset_status_grouped IN ('LOST SOLVED','LOST') 
  	 THEN updated_date 
   END,(DATEADD('MONTH',1,DATE_TRUNC('MONTH',CURRENT_DATE))-1)::DATE)
 AND a.asset_status_grouped NOT IN ('NEVER PURCHASED')
;

--------------------------------ASSET COHORT MONTH EVENTS-----------------------------------------------

DROP TABLE IF EXISTS trans_dev.asset_cohort_month_events;
CREATE TABLE trans_dev.asset_cohort_month_events AS
WITH delinquency_event AS (
/*#############################################
Define Delinquency AND Default Events
#############################################*/
SELECT DISTINCT 
  a.asset_id
 ,sp.customer_id
 ,(sp.due_date+31)::DATE AS delinquency_date
 ,(CASE WHEN d.dpd>=61 THEN sp.due_date+61 END)::DATE AS soft_default_date
 ,(CASE WHEN d.dpd>=91 THEN sp.due_date+91 END)::DATE AS hard_default_date
 ,(CASE WHEN d.dpd>=361 THEN sp.due_date+361 END)::DATE AS hard_360_default_date
 ,(CASE WHEN d.dpd>=721 THEN sp.due_date+721 END)::DATE AS hard_720_default_date
 ,d.subscription_payment_category
 ,sp.asset_return_date
FROM ods_production.asset a 
LEFT JOIN ods_production.payment_subscription sp 
  ON a.asset_id = sp.asset_id
LEFT JOIN ods_production.payment_subscription_details d 
  ON sp.subscription_payment_id = d.subscription_payment_id
 AND COALESCE(d.paid_date,'1990-05-22')= COALESCE(sp.paid_date,'1990-05-22')
WHERE TRUE
  AND sp.first_asset_delivery_date IS NOT NULL 
  AND sp.due_date IS NOT NULL 
  AND d.subscription_payment_category LIKE ('%DEFAULT%') 
  AND ((sp.due_date+31)::DATE < sp.asset_return_date or sp.asset_return_date IS NULL)
)
, curing_event AS (
/*#############################################
Curing Events
#############################################*/
SELECT DISTINCT 
  a.asset_id
 ,sp.paid_date::DATE AS curing_date
 ,'payment recovery'as curing_type
 ,COUNT(DISTINCT sp.due_date ) AS cured_events
 ,COUNT(DISTINCT CASE WHEN d.dpd>=61 THEN sp.due_date END) AS cured_events_soft_default
 ,COUNT(DISTINCT CASE WHEN d.dpd>=91 THEN sp.due_date END) AS cured_events_hard_default
 ,COUNT(DISTINCT CASE WHEN d.dpd>=361 THEN sp.due_date END) AS cured_events_360_default
 ,COUNT(DISTINCT CASE WHEN d.dpd>=721 THEN sp.due_date END) AS cured_events_720_default
FROM ods_production.asset a 
  LEFT JOIN ods_production.payment_subscription sp 
    ON a.asset_id=sp.asset_id
  LEFT JOIN ods_production.payment_subscription_details d 
   ON sp.subscription_payment_id=d.subscription_payment_id
  AND COALESCE(d.paid_date,'1990-05-22')=COALESCE(sp.paid_date,'1990-05-22')
WHERE TRUE
  AND sp.first_asset_delivery_date IS NOT NULL 
  AND sp.due_date IS NOT NULL 
  AND d.subscription_payment_category LIKE ('%DEFAULT%') 
  AND d.subscription_payment_category LIKE ('%RECOVERY%')
  AND (sp.paid_date::DATE < sp.asset_return_date::DATE 
       OR sp.asset_return_date::DATE IS NULL)
GROUP BY 1,2,3
UNION ALL
SELECT DISTINCT 
  a.asset_id
 ,sp.asset_return_date::DATE AS curing_date
 ,'asset return'as curing_type
 ,COUNT(DISTINCT CASE 
   WHEN (sp.due_date+31)::DATE < sp.asset_return_date 
    THEN sp.due_date 
  END) AS cured_events
 ,COUNT(DISTINCT CASE 
   WHEN (sp.due_date+61)::DATE < sp.asset_return_date 
    THEN sp.due_date 
  END) AS cured_events_soft_default
 ,COUNT(DISTINCT CASE 
   WHEN (sp.due_date+91)::DATE < sp.asset_return_date 
    THEN sp.due_date 
  END) AS cured_events_hard_default
 ,COUNT(DISTINCT CASE 
   WHEN (sp.due_date+361)::DATE < sp.asset_return_date 
    THEN sp.due_date 
  END) AS cured_events_360_default
 ,COUNT(DISTINCT CASE 
   WHEN (sp.due_date+721)::DATE < sp.asset_return_date 
    THEN sp.due_date 
  END) AS cured_events_720_default
FROM ods_production.asset a 
  LEFT JOIN ods_production.payment_subscription sp 
    ON a.asset_id=sp.asset_id
  LEFT JOIN ods_production.payment_subscription_details d 
    ON sp.subscription_payment_id=d.subscription_payment_id
   AND COALESCE(d.paid_date,'1990-05-22')=COALESCE(sp.paid_date,'1990-05-22')
WHERE TRUE
  AND sp.first_asset_delivery_date IS NOT NULL 
  AND sp.due_date IS NOT NULL 
  AND d.subscription_payment_category LIKE ('%DEFAULT%') 
  AND sp.asset_return_date::DATE  IS NOT NULL
  AND ((sp.due_date+31)::DATE < sp.asset_return_date or sp.asset_return_date IS NULL)
GROUP BY 1,2,3
)
,sold_event AS (
/*#############################################
SOLD Events
#############################################*/
SELECT DISTINCT 
  asset_id
 ,sold_date::DATE AS sold_date
FROM ods_production.asset
WHERE sold_date::DATE IS NOT NULL
)
,lost_event AS(
SELECT DISTINCT 
  asset_id
 ,COALESCE(lost_date::DATE ,updated_date::DATE) AS lost_date
FROM ods_production.asset
WHERE TRUE 
  AND asset_status_grouped IN ('LOST SOLVED','LOST') 
  AND sold_date IS NULL
)
SELECT 
  s.*
 ,SUM(CASE 
   WHEN a.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS delinquency_event
 ,SUM(CASE 
   WHEN a2.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS soft_default_event
 ,SUM(CASE 
   WHEN a3.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS hard_default_event
 ,SUM(CASE 
   WHEN a4.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS hard_360_default_event
 ,SUM(CASE 
   WHEN a5.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS hard_720_default_event
 ,COALESCE(SUM(b.cured_events),0) AS curing_event
 ,COALESCE(SUM(b.cured_events_soft_default),0) AS curing_event_soft_default
 ,COALESCE(SUM(b.cured_events_hard_default),0) AS curing_event_hard_default
 ,COALESCE(SUM(b.cured_events_360_default),0) AS curing_event_360_default
 ,COALESCE(SUM(b.cured_events_720_default),0) AS curing_event_720_default
 ,SUM(CASE 
   WHEN c.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
  END) AS sold_event
 ,SUM(CASE 
   WHEN d.asset_id IS NOT NULL 
    THEN 1 
   ELSE 0 
 END) AS lost_event
FROM trans_dev.asset_cohort_month s
  LEFT JOIN delinquency_event a 
    ON s.asset_id=a.asset_id
   AND s.datum=a.delinquency_date
  LEFT JOIN delinquency_event a2
    ON s.asset_id=a2.asset_id
   AND s.datum=a2.soft_default_date
  LEFT JOIN delinquency_event a3
    ON s.asset_id=a3.asset_id
   AND s.datum=a3.hard_default_date
  LEFT JOIN delinquency_event a4
    ON s.asset_id=a4.asset_id
   AND s.datum=a4.hard_360_default_date
  LEFT JOIN delinquency_event a5
    ON s.asset_id=a5.asset_id
   AND s.datum=a5.hard_720_default_date
  LEFT JOIN curing_event b
    ON s.asset_id=b.asset_id
   AND s.datum=b.curing_date
  LEFT JOIN sold_event c
    ON s.asset_id=c.asset_id
   AND s.datum=c.sold_date
  LEFT JOIN lost_event d
    ON s.asset_id=d.asset_id
   AND s.datum=d.lost_date
 GROUP BY s.asset_id, s.cohort, s.cohort_month, s.shifted_creation_cohort, s.datum, s.datum_month --s.months_since_acquisition
;

--------------------------------ASSET UTLIZATION-----------------------------------------------

DROP TABLE IF EXISTS trans_dev.asset_utilization;
CREATE TABLE trans_dev.asset_utilization AS
WITH s AS (
SELECT 
  a.asset_id 
 ,a.purchased_date::DATE AS cohort 
 ,d.datum  
 ,DATE_TRUNC('MONTH',d.datum)::DATE AS datum_month   
FROM ods_production.asset a, public.dim_dates d
WHERE a.purchased_date <= d.datum 
  AND d.datum <= COALESCE(sold_date,lost_date,
   CASE 
  	WHEN asset_status_grouped IN ('LOST SOLVED','LOST') 
  	 THEN updated_date 
   END,(DATEADD('MONTH',1,DATE_TRUNC('MONTH',CURRENT_DATE))-1)::DATE)
  AND a.asset_status_grouped NOT IN ('NEVER PURCHASED')
)
, b AS (
SELECT DISTINCT 
   a.asset_id
  ,a.allocation_id
  ,LEAST(a.created_at,a.allocated_at)::DATE AS START_DATE
  ,COALESCE(aa.return_delivered__c, aa.return_picked_by_carrier__c,aa.cancelltion_returned__c,
  	CASE 
  	 WHEN aa.status__c ='RETURNED' 
  	  THEN aa.subscription_cancellation__c 
  	END) AS end_date
 ,(DATEADD('MONTH',1,DATE_TRUNC('MONTH',CURRENT_DATE))-1)::DATE AS end_of_current_month
FROM ods_production.allocation a
  LEFT JOIN stg_salesforce.customer_asset_allocation__c aa 
    ON a.allocation_id = aa.id
WHERE TRUE 
AND a.delivered_at IS NOT NULL 
)
SELECT 
  s.asset_id
 ,s.cohort
 ,s.datum_month
 ,MIN(s.datum) AS cohort_start
 ,MAX(s.datum) AS cohort_end
 ,(MAX(s.datum)-MIN(s.datum)) AS dates
 ,SUM(CASE WHEN b.asset_id IS NOT NULL THEN 1 ELSE 0 END) AS days_utilized
 ,CASE 
   WHEN SUM(CASE 
   	WHEN b.asset_id IS NOT NULL 
   	 THEN 1 
   	ELSE 0 
   END)::DECIMAL/ GREATEST((MAX(s.datum) - MIN(s.datum)),1)::DECIMAL >= 0.5 
   THEN 1 
  ELSE 0 
 END AS is_utilized
FROM s
  LEFT JOIN b 
    ON s.asset_id = b.asset_id
   AND s.datum >= start_date 
   AND s.datum <= COALESCE(end_date,end_of_current_month)
GROUP BY 1,2,3;

--------------------------------RUNNING SUM EVENTS-----------------------------------------------

DROP TABLE IF EXISTS trans_dev.curing_events;
CREATE TABLE trans_dev.curing_events AS 
SELECT 
c.asset_id
,c.cohort_month
,c.shifted_creation_cohort
,c.datum
,c.datum_month
,c.delinquency_event
,c.soft_default_event
,c.hard_default_event
,c.hard_360_default_event
,c.hard_720_default_event
,c.curing_event
,c.curing_event_soft_default
,c.curing_event_hard_default
,c.curing_event_360_default
,c.curing_event_720_default
,c.sold_event
,c.lost_event
,SUM(c.delinquency_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_delinquency
,SUM(c.soft_default_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_soft_default
,SUM(c.hard_default_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_hard_default
,SUM(c.hard_360_default_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_360_default
,SUM(c.hard_720_default_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_720_default
,SUM(c.curing_event) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_curing
,SUM(c.curing_event_soft_default) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_curing_soft_default
,SUM(c.curing_event_hard_default) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_curing_hard_default
,SUM(c.curing_event_360_default) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_curing_360_default
,SUM(c.curing_event_720_default) OVER (PARTITION BY c.asset_id ORDER BY c.datum ROWS UNBOUNDED PRECEDING) AS running_curing_720_default
FROM trans_dev.asset_cohort_month_events c;


--------------------------------DPD EVENTS-----------------------------------------------

DROP TABLE IF EXISTS trans_dev.performing;
CREATE TABLE trans_dev.performing AS 
WITH a AS (
SELECT 
asset_id
,cohort_month
,shifted_creation_cohort
,datum_month
,SUM(delinquency_event) AS delinquency_event
,SUM(soft_default_event) AS soft_default_event
,SUM(hard_default_event) AS hard_default_event
,SUM(hard_360_default_event) AS hard_360_default_event
,SUM(hard_720_default_event) AS hard_720_default_event
,SUM(curing_event) AS curing_event
,SUM(curing_event_soft_default) AS curing_event_soft_default
,SUM(curing_event_hard_default) AS curing_event_hard_default
,SUM(curing_event_360_default) AS curing_event_360_default
,SUM(curing_event_720_default) AS curing_event_720_default
,SUM(sold_event) AS sold_event
,SUM(lost_event) AS lost_event
,MAX(running_delinquency) AS running_delinquency
,MAX(running_soft_default) AS running_soft_default
,MAX(running_hard_default) AS running_hard_default
,MAX(running_360_default) AS running_360_default
,MAX(running_720_default) AS running_720_default
,MAX(running_curing) AS running_curing
,MAX(running_curing_soft_default) AS running_curing_soft_default
,MAX(running_curing_hard_default) AS running_curing_hard_default
,MAX(running_curing_360_default) AS running_curing_360_default 
,MAX(running_curing_720_default) AS running_curing_720_default 
FROM trans_dev.curing_events
WHERE true
GROUP BY 1,2,3,4
)
,b AS (
SELECT 
  asset_id
 ,cohort_month
 ,shifted_creation_cohort
 ,datum_month
 ,CASE 
   WHEN running_delinquency = 0 
     AND running_curing = 0  -- months before any delinquency AND curing event
    THEN 1  
   WHEN delinquency_event= curing_event 
     AND running_delinquency= running_curing -- months WHEN delinquent AND cured the same period 
    THEN 1
   WHEN LAG(running_delinquency) OVER (PARTITION BY asset_id ORDER BY datum_month) = 
        LAG(running_curing) OVER (PARTITION BY asset_id ORDER BY datum_month)
     AND delinquency_event >= 1
    THEN 1 -- just turned delinquent
   WHEN LAG(running_delinquency) OVER (PARTITION BY asset_id ORDER BY datum_month) > 
        LAG(running_curing) OVER (PARTITION BY asset_id ORDER BY datum_month)
    THEN 0 -- turnerd delinquent prevuious month AND did NOT cure this month
   WHEN running_curing >= running_delinquency -- repaid everything late AND returned the asset
    THEN 1
   WHEN datum_month = cohort_month 
     AND delinquency_event =1
    THEN 1
   ELSE 1 
  END AS performing 
 ,CASE 
   WHEN running_soft_default = 0 
     AND running_curing_soft_default = 0  -- months before any delinquency AND curing event
    THEN 1  
   WHEN soft_default_event=curing_event_soft_default 
     AND running_soft_default=running_curing_soft_default -- months WHEN delinquent AND cured the same period 
    THEN 1
   WHEN LAG(running_soft_default) OVER (PARTITION BY asset_id ORDER BY datum_month) = 
        LAG(running_curing_soft_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
     AND soft_default_event >= 1
    THEN 1 -- just turned delinquent
   WHEN LAG(running_soft_default) OVER (PARTITION BY asset_id ORDER BY datum_month) > 
        LAG(running_curing_soft_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
    THEN 0 -- turnerd delinquent prevuious month AND did NOT cure this month
   WHEN running_curing_soft_default >= running_soft_default -- repaid everything late AND returned the asset
    THEN 1
   WHEN datum_month = cohort_month 
     AND soft_default_event =1
    THEN 1
   ELSE 1 
  END AS performing_soft_default
 ,CASE 
   WHEN running_hard_default = 0 
     AND running_curing_hard_default = 0  -- months before any delinquency AND curing event
    THEN 1  
   WHEN hard_default_event=curing_event_hard_default
     AND running_hard_default=running_curing_hard_default -- months WHEN delinquent AND cured the same period 
    THEN 1
   WHEN LAG(running_hard_default) OVER (PARTITION BY asset_id ORDER BY datum_month) = 
        LAG(running_curing_hard_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
     AND hard_default_event >= 1
    THEN 1 -- just turned delinquent
   WHEN LAG(running_hard_default) OVER (PARTITION BY asset_id ORDER BY datum_month) > 
        LAG(running_curing_hard_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
    THEN 0 -- turnerd delinquent prevuious month AND did NOT cure this month
   WHEN running_curing_hard_default >= running_hard_default -- repaid everything late AND returned the asset
    THEN 1
   WHEN datum_month = cohort_month 
     AND hard_default_event = 1
    THEN 1
   ELSE 1 
  END AS performing_hard_default
 ,CASE 
  WHEN running_360_default = 0 
    AND running_curing_360_default = 0  -- months before any delinquency and curing event
   THEN 1  
  WHEN hard_360_default_event=curing_event_360_default
    AND running_360_default=running_curing_360_default -- months when delinquent and cured the same period 
   THEN 1
  WHEN LAG(running_360_default) OVER (PARTITION BY asset_id ORDER BY datum_month) = 
       LAG(running_curing_360_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
    AND hard_360_default_event >= 1
   THEN 1 -- just turned delinquent
  WHEN LAG(running_360_default) OVER (PARTITION BY asset_id ORDER BY datum_month) > 
       LAG(running_curing_360_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
   THEN 0 -- turnerd delinquent prevuious month and did not cure this month
  WHEN running_curing_360_default >= running_360_default -- repaid everything late and returned the asset
   THEN 1
  WHEN datum_month = cohort_month 
    AND hard_360_default_event =1
   THEN 1
  ELSE 1 
 END AS performing_360_default
 ,CASE 
   WHEN running_720_default = 0 
     AND running_curing_720_default = 0  -- months before any delinquency AND curing event
    THEN 1  
   WHEN hard_720_default_event= curing_event_720_default 
     AND running_720_default= running_curing_720_default -- months WHEN delinquent AND cured the same period 
    THEN 1
   WHEN LAG(running_720_default) OVER (PARTITION BY asset_id ORDER BY datum_month) = 
        LAG(running_curing_720_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
     AND hard_720_default_event >= 1
    THEN 1 -- just turned delinquent
   WHEN LAG(running_720_default) OVER (PARTITION BY asset_id ORDER BY datum_month) > 
        LAG(running_curing_720_default) OVER (PARTITION BY asset_id ORDER BY datum_month)
    THEN 0 -- turnerd delinquent prevuious month AND did NOT cure this month
   WHEN running_curing_720_default >= running_720_default -- repaid everything late AND returned the asset
    THEN 1
   WHEN datum_month = cohort_month 
     AND hard_720_default_event = 1
    THEN 1
   ELSE 1 
  END AS performing_720_default
 ,delinquency_event
 ,soft_default_event
 ,hard_default_event
 ,hard_360_default_event
 ,hard_720_default_event
 ,curing_event
 ,curing_event_soft_default
 ,curing_event_hard_default
 ,curing_event_360_default
 ,curing_event_720_default
 ,running_delinquency
 ,running_soft_default
 ,running_hard_default
 ,running_360_default
 ,running_720_default
 ,running_curing
 ,running_curing_soft_default
 ,running_curing_hard_default
 ,running_curing_360_default
 ,running_curing_720_default
 ,sold_event
 ,lost_event
FROM a) 
SELECT 
  b.asset_id
 ,b.cohort_month
 ,b.shifted_creation_cohort
 ,b.datum_month
 ,b.running_delinquency
 ,b.running_curing
 ,b.curing_event
,CASE 
   WHEN LAG(CASE 
    WHEN b.performing = 1 
      AND b.delinquency_event = 1 
     THEN 1 
    ELSE 0 
   END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
    AND LAG(CASE WHEN b.running_delinquency <= b.running_curing 
    AND b.curing_event > 0 
   THEN 1 
  ELSE 0 
 END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
   THEN 0 
  WHEN LAG(b.performing) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    AND LAG(CASE 
      WHEN b.running_delinquency <= b.running_curing 
        AND b.curing_event > 0 
       THEN 1 
      ELSE 0 
     END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
   THEN 0
  ELSE b.performing 
 END AS performing
 ,CASE 
   WHEN LAG(CASE 
     WHEN b.performing_soft_default=1 
       AND b.soft_default_event = 1 
      THEN 1 
     ELSE 0 
    END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG(CASE 
       WHEN b.running_soft_default <= b.running_curing_soft_default 
         AND b.curing_event_soft_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_soft_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_soft_default <= b.running_curing_soft_default 
         AND b.curing_event_soft_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0
   ELSE b.performing_soft_default 
  END AS performing_soft_default
 ,CASE 
   WHEN LAG(CASE 
     WHEN b.performing_hard_default=1 
       AND b.hard_default_event = 1 
      THEN 1 
     ELSE 0 
    END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG(CASE 
       WHEN b.running_hard_default <= b.running_curing_hard_default 
         AND b.curing_event_hard_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_hard_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_hard_default <= b.running_curing_hard_default 
         AND b.curing_event_hard_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0
   ELSE b.performing_hard_default 
  END AS performing_hard_default
 ,CASE 
   WHEN LAG(CASE 
     WHEN b.performing_360_default=1 
       AND b.hard_360_default_event = 1 
      THEN 1 
     ELSE 0 
    END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG(CASE 
       WHEN b.running_360_default <= b.running_curing_360_default 
         AND b.curing_event_360_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_360_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_360_default <= b.running_curing_360_default 
         AND b.curing_event_360_default >0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)=0
    THEN 0
   ELSE b.performing_360_default 
  END AS performing_360_default
 ,CASE 
   WHEN LAG(CASE 
     WHEN b.performing_720_default = 1 
       AND b.hard_720_default_event = 1 
      THEN 1 
     ELSE 0 
    END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)=1
     AND LAG(CASE 
       WHEN b.running_720_default <= b.running_curing_720_default 
         AND b.curing_event_720_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
    THEN 0 
   WHEN LAG(b.performing_720_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_720_default <= b.running_curing_720_default 
         AND b.curing_event_720_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
    THEN 0
   ELSE b.performing_720_default 
  END AS performing_720_default
 ,CASE 
   WHEN LAG((CASE 
     WHEN b.performing = 1 
       AND b.delinquency_event = 1 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG((CASE 
       WHEN b.running_delinquency <= b.running_curing 
         AND b.curing_event > 0 
        THEN 1 
       ELSE 0 
      END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
    THEN 0 
   WHEN LAG(b.performing) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_delinquency <= b.running_curing 
         AND b.curing_event > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
    THEN 0
   ELSE (CASE 
     WHEN b.performing = 1 
     AND b.delinquency_event = 1 
    THEN 1 
     ELSE 0 
    END) 
  END AS delinquency_new
 ,CASE 
   WHEN LAG((CASE 
     WHEN b.performing_soft_default = 1 
       AND b.soft_default_event = 1 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG((CASE 
     WHEN b.running_soft_default <= b.running_curing_soft_default 
       AND b.curing_event_soft_default > 0 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_soft_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
     WHEN b.running_soft_default <= b.running_curing_soft_default 
       AND b.curing_event_soft_default > 0 
      THEN 1 
     ELSE 0 
    END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0
   ELSE (CASE 
     WHEN b.performing_soft_default = 1 
       AND b.soft_default_event = 1 
      THEN 1 
     ELSE 0 
    END) 
  END AS soft_default_new
 ,CASE 
   WHEN LAG((CASE 
     WHEN b.performing_hard_default=1 
       AND b.hard_default_event = 1 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG((CASE 
      WHEN b.running_hard_default <= b.running_curing_hard_default 
        AND b.curing_event_hard_default > 0 
       THEN 1 
      ELSE 0 
     END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_hard_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
    WHEN b.running_hard_default <= b.running_curing_hard_default 
      AND b.curing_event_hard_default > 0 
     THEN 1 
    ELSE 0 
   END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
   THEN 0
  ELSE (CASE 
    WHEN b.performing_hard_default=1 
      AND b.hard_default_event = 1 
     THEN 1 
    ELSE 0 
   END) 
 END AS hard_default_new
 ,CASE 
   WHEN LAG((CASE 
     WHEN b.performing_360_default= 1 
       AND b.hard_360_default_event = 1 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)=1
     AND LAG((CASE 
      WHEN b.running_360_default <= b.running_curing_360_default 
        AND b.curing_event_360_default >0 
       THEN 1 
      ELSE 0 
     END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)=0
    THEN 0 
   WHEN LAG(b.performing_360_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_360_default <= b.running_curing_360_default 
         AND b.curing_event_360_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)=0
   THEN 0
  ELSE (CASE 
    WHEN b.performing_360_default=1 
      AND b.hard_360_default_event = 1 
     THEN 1 
    ELSE 0 
   END) 
 END AS hard_360_default_new
 ,CASE 
   WHEN LAG((CASE 
     WHEN b.performing_720_default=1 
       AND b.hard_720_default_event = 1 
      THEN 1 
     ELSE 0 
    END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 1
     AND LAG((CASE 
       WHEN b.running_720_default <= b.running_curing_720_default 
         AND b.curing_event_720_default > 0 
        THEN 1 
       ELSE 0 
      END)) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
    THEN 0 
   WHEN LAG(b.performing_720_default) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC) = 0
     AND LAG(CASE 
       WHEN b.running_720_default <= b.running_curing_720_default 
         AND b.curing_event_720_default > 0 
        THEN 1 
       ELSE 0 
      END) OVER (PARTITION BY b.asset_id ORDER BY b.datum_month ASC)= 0
    THEN 0
   ELSE (CASE 
     WHEN b.performing_720_default=1 
       AND b.hard_720_default_event = 1 
      THEN 1 
     ELSE 0 
    END) 
  END AS hard_720_default_new
 ,CASE 
   WHEN b.running_delinquency <= b.running_curing 
     AND b.curing_event > 0 
    THEN 1 
   ELSE 0 
  END AS curing_new
 ,CASE 
   WHEN b.running_soft_default <= b.running_curing_soft_default
     AND b.curing_event_soft_default > 0 
    THEN 1 
   ELSE 0 
  END AS curing_new_soft_default
 ,CASE 
  WHEN b.running_hard_default <= b.running_curing_hard_default
   AND b.curing_event_hard_default > 0 
   THEN 1 
  ELSE 0 
  END AS curing_new_hard_default
 ,CASE 
  WHEN b.running_360_default <= b.running_curing_360_default
   AND b.curing_event_360_default >0 
   THEN 1 
  ELSE 0 
  END AS curing_new_360_default
 ,CASE 
   WHEN b.running_720_default <= b.running_curing_720_default
     AND b.curing_event_720_default > 0 
    THEN 1 
   ELSE 0 
  END AS curing_new_720_default
  ,CASE 
   WHEN ac.name='DHL' 
     AND LEAST(b.sold_event,1) = 1 
    THEN 0 
   ELSE LEAST(b.sold_event,1) 
  END AS sold
 ,CASE 
   WHEN ac.name= 'DHL' 
     AND LEAST(b.sold_event,1) = 1 
    THEN 1 
   ELSE LEAST(b.lost_event,1) 
  END AS lost
  FROM b 
  LEFT JOIN stg_salesforce.asset asf 
    ON b.asset_id = asf.id
  LEFT JOIN stg_salesforce.account ac  
    ON ac.id = asf.accountid;

--------------------------------ASSET COLLECTION CURVE-----------------------------------------------   

DROP TABLE IF EXISTS dwh.asset_collection_curves;
CREATE TABLE dwh.asset_collection_curves AS
WITH cashflow AS (
SELECT DISTINCT 
  pa.asset_id
 ,GREATEST(DATE_TRUNC('MONTH',pa.paid_date),DATE_TRUNC('MONTH',a.purchased_date))::DATE AS paid_month
 ,SUM(CASE 
   WHEN pa.payment_type = 'REPAIR COST' 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid 
   ELSE 0 
  END) AS repair_cost_paid
 ,SUM(CASE 
   WHEN pa.payment_type IN ('FIRST','RECURRENT') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS subscription_revenue_paid
 ,SUM(CASE 
   WHEN pa.payment_type IN ('SHIPMENT') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS shipment_cost_paid
 ,SUM(CASE 
   WHEN pa.payment_type IN ('CUSTOMER BOUGHT') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS customer_bought_paid
 ,SUM(CASE 
   WHEN pa.payment_type IN ('GROVER SOLD') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
 ,SUM(CASE 
   WHEN pa.payment_type IN ('DEBT COLLECTION') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS debt_collection_paid
 ,SUM(CASE 
   WHEN pa.payment_type IN ('ADDITIONAL CHARGE','COMPENSATION') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS additional_charge_paid
 ,SUM(CASE 
   WHEN pa.payment_type LIKE ('%CHARGE BACK%') 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS chargeback_paid
 ,SUM(CASE 
   WHEN pa.payment_type LIKE '%REFUND%' 
     AND pa.status = 'PAID' 
    THEN pa.amount_paid  
   ELSE 0 
  END) AS refunds_paid
FROM ods_production.payment_all pa 
  LEFT JOIN ods_production.asset a 
    ON pa.asset_id=a.asset_id
GROUP BY 1,2
)
,asset_customer_split AS (
SELECT DISTINCT
  ah.date AS datum
  ,ah.asset_id
  ,COALESCE(al.customer_id::TEXT, o.customer_id::TEXT) AS customer_id  
  ,o.new_recurring 
  ,o.customer_type 
FROM master.asset_historical ah 
  LEFT JOIN ods_production.allocation al
    ON ah.asset_allocation_id = al.allocation_id
  LEFT JOIN master.order o
    ON al.order_id = o.order_id 
 WHERE ah.date=date_trunc('month',ah.date)
)
,allocated_assets AS (
SELECT 
  aa.asset_id
 ,DATE_TRUNC('MONTH',aa.allocated_at)::DATE AS allocated_month
 ,COUNT(DISTINCT aa.allocation_id) AS allocated_assets
 ,SUM(s.subscription_value) AS allocated_sub_value
 ,SUM(a.initial_price) AS allocated_purchase_price
FROM ods_production.allocation aa 
  LEFT JOIN ods_production.subscription s 
    ON aa.subscription_id=s.subscription_id
  LEFT JOIN ods_production.asset a 
    ON a.asset_id=aa.asset_id
GROUP BY 1,2
)
,asset_valuation AS (
SELECT 
  asset_id
 ,DATE_TRUNC('MONTH',reporting_date)::DATE AS report_date 
 ,MAX(final_price) AS final_price
 ,MAX(prev_final_price) AS prev_final_price
FROM dm_finance.spv_report_historical srm 
WHERE srm.reporting_date = LAST_DAY(reporting_date)
GROUP BY 1,2
)
,dpd_month_end AS (
SELECT 
  asset_id
  ,date
 ,DATE_TRUNC('month', date)::DATE AS reporting_date
 ,last_allocation_dpd
FROM master.asset_historical ah 
WHERE TRUE
   AND date = LAST_DAY(date)
   QUALIFY ROW_NUMBER() over (PARTITION BY asset_id,reporting_date ORDER BY last_allocation_dpd DESC )=1
)
,capital_source_name_first_month_on_book  AS (
SELECT 
    ah.asset_id, 
    ah.capital_source_name
FROM master.asset_historical ah 
JOIN (
        SELECT 
            asset_id , 
            MIN(ah.date) AS min_date
        FROM master.asset_historical ah 
        GROUP BY asset_id
) sub 
ON ah.asset_id = sub.asset_id 
AND ah.date = sub.min_date
)
,asset_historical AS (-- this IS  done TO combat the duplicates IN asset historical table
	SELECT 
	DISTINCT 
		date,
		asset_id,
		asset_status_original,
		capital_source_name,
		ah.country ,
		ah.shipping_country 
	FROM master.asset_historical ah
  WHERE (date=date_trunc('month',date) OR date=last_day(date)) 
)
SELECT DISTINCT 
  b.asset_id
 ,b.cohort_month
 ,b.shifted_creation_cohort
 ,b.datum_month
 ,cust.customer_type
 ,cust.new_recurring AS new_recurring_Mitja
 ,DATEDIFF('MONTH', b.cohort_month, b.datum_month) AS months_on_book
 ,DATEDIFF('month',b.shifted_creation_cohort, b.datum_month ) AS months_since_shifted_creation_cohort
 ,b.performing
 ,b.performing_soft_default
 ,b.performing_hard_default
 ,b.performing_360_default
 ,b.performing_720_default
 ,b.delinquency_new
 ,b.soft_default_new
 ,b.hard_default_new
 ,b.hard_360_default_new
 ,b.hard_720_default_new
 ,b.curing_new
 ,b.curing_new_soft_default
 ,b.curing_new_hard_default
 ,b.curing_new_360_default
 ,b.curing_new_720_default
 ,b.sold
 ,b.lost
 ,a.initial_price
 ,vv.final_price AS market_valuation
 ,vv.prev_final_price AS prev_market_valuation
 ,COALESCE(i.repair_cost_paid,0) AS repair_cost_paid
 ,COALESCE(i.subscription_revenue_paid,0) AS subscription_revenue_paid
 ,COALESCE(i.shipment_cost_paid,0) AS shipment_cost_paid
 ,COALESCE(i.customer_bought_paid,0) AS customer_bought_paid
 ,COALESCE(i.additional_charge_paid,0) AS additional_charge_paid
 ,COALESCE(i.debt_collection_paid,0) AS debt_collection_paid
 ,COALESCE(i.chargeback_paid,0) AS chargeback_paid
 ,COALESCE(i.refunds_paid,0) AS refunds_paid
 ,COALESCE(i.repair_cost_paid,0)+
  COALESCE(i.subscription_revenue_paid,0)+
  COALESCE(i.shipment_cost_paid,0)+
  COALESCE(i.customer_bought_paid,0)+
  COALESCE(i.additional_charge_paid,0)+
  COALESCE(i.debt_collection_paid,0)+
  COALESCE(i.chargeback_paid,0)+
  COALESCE(i.refunds_paid,0) AS net_amount_paid
 ,SUM(COALESCE(i.repair_cost_paid,0)+
  COALESCE(i.subscription_revenue_paid,0)+
  COALESCE(i.shipment_cost_paid,0)+
  COALESCE(i.customer_bought_paid,0)+
  COALESCE(i.additional_charge_paid,0)+
  COALESCE(i.debt_collection_paid,0)+
  COALESCE(i.chargeback_paid,0)+
  COALESCE(i.refunds_paid,0)) 
  OVER (PARTITION BY b.asset_id ORDER BY b.datum_month DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS cumuative_amount_paid
 ,COALESCE(ah.capital_source_name,cf.capital_source_name) AS capital_source_name 
 ,a.warehouse
 ,a.first_allocation_store
 ,a.category_name
 ,a.subcategory_name
 ,a.brAND
 ,a.product_sku
 ,a.variant_sku
 ,a.asset_status_new
 ,CASE 
   WHEN a.capital_source_name = 'USA_test' 
    THEN 'United States'
   ELSE COALESCE(apc.country_name, ascc.country_name ,ah.country , ah.shipping_country ) 
  END AS country
 ,ah.asset_status_original 
 ,u.days_utilized
 ,GREATEST(u.is_utilized, 
   CASE 
    WHEN b.running_delinquency <= b.running_curing 
      AND b.curing_event > 0 
     THEN 1 
    ELSE 0 
   END) AS is_utilized
 ,al.allocated_assets
 ,al.allocated_sub_value
 ,al.allocated_purchase_price
 ,dme.last_allocation_dpd
 ,CASE 
   WHEN a.asset_status_new = 'SOLD' 
    THEN a.asset_status_detailed 
   ELSE NULL 
  END AS sold_status
FROM trans_dev.performing b
  LEFT JOIN master.asset a 
    ON a.asset_id = b.asset_id
  LEFT JOIN cashflow i 
    ON a.asset_id = i.asset_id 
   AND i.paid_month = b.datum_month
  LEFT JOIN trans_dev.asset_utilization u
    ON u.asset_id = b.asset_id
   AND u.datum_month::DATE=b.datum_month::DATE
  LEFT JOIN allocated_assets al  
    ON al.asset_id = b.asset_id
   AND al.allocated_month=b.datum_month::DATE
  LEFT JOIN asset_valuation vv
    ON vv.asset_id = b.asset_id
   AND vv.report_date=b.datum_month::DATE
  LEFT JOIN asset_customer_split cust 
    ON b.asset_id = cust.asset_id
   AND cust.datum::DATE = b.datum_month::DATE
  LEFT JOIN trans_dev.asset_payment_country apc
    ON apc.asset_id = a.asset_id 
   AND apc."date"::DATE = b.datum_month::DATE
  LEFT JOIN asset_historical ah 
    ON ah.asset_id = b.asset_id
   AND ah."date"::DATE = CASE  
    WHEN b.datum_month = DATE_TRUNC('month', CURRENT_DATE)::DATE 
     THEN CURRENT_DATE -1 
    ELSE LAST_DAY(b.datum_month::DATE)
   END
  LEFT JOIN dpd_month_end dme 
    ON b.asset_id = dme.asset_id
   AND b.datum_month::DATE = dme.reporting_date   
  LEFT JOIN capital_source_name_first_month_on_book cf 
    ON b.asset_id = cf.asset_id
  LEFT JOIN trans_dev.asset_subscription_country ascc
    ON ascc.asset_id = b.asset_id
   AND ascc.datum_month::DATE = b.datum_month::DATE;

GRANT SELECT ON dwh.asset_collection_curves TO debt_management_redash;
GRANT SELECT ON dwh.asset_collection_curves TO elene_tsintsadze;
GRANT SELECT ON dwh.asset_collection_curves TO tableau;

