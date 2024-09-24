DROP VIEW IF EXISTS marketing.v_affiliate_order_validation;
CREATE VIEW marketing.v_affiliate_order_validation AS
/*THIS CTE WILL COMBINE ALL SOURCE DATA FROM DIFFERENT AFFILIATE NETWORKS*/
WITH 
tradedoubler_clicks_and_affiliate as (
    SELECT DISTINCT 
        session_id,  
        event_time, 
        CASE WHEN LOWER(marketing_source) = 'tradedoubler' THEN REPLACE(REPLACE(REPLACE(marketing_campaign, '+', ' '), '%28', '('), '%29', ')') ELSE NULL END AS marketing_campaign,
        CASE WHEN LOWER(traits_source) = 'tradedoubler' THEN REPLACE(REPLACE(REPLACE(traits_campaign, '+', ' '), '%28', '('), '%29', ')')  ELSE NULL END AS traits_campaign,
        COALESCE(CASE WHEN page_url LIKE '%tduid%'
                      THEN SPLIT_PART(SPLIT_PART(page_url,'tduid=',2),'&',1) END,
                 CASE WHEN page_referrer LIKE '%tduid%'
                      THEN SPLIT_PART(SPLIT_PART(page_referrer,'tduid=',2),'&',1) END) AS td_click_id
    FROM segment.all_events
    WHERE td_click_id IS NOT NULL 
        AND event_time >= CURRENT_DATE - INTERVAL '3 month')
    
,rank_td_clicks_and_orders AS (
    SELECT a.order_id,
           b.session_id,
           b.event_time,
           c.store_country,
           coalesce(b.marketing_campaign,b.traits_campaign) AS marketing_campaign,
           td_click_id AS click_id,
           ROW_NUMBER() OVER (PARTITION BY a.order_id ORDER BY event_time DESC) AS rn
    FROM traffic.session_order_mapping a
             LEFT JOIN tradedoubler_clicks_and_affiliate b using(session_id)
             LEFT JOIN master."order" c on a.order_id = c.order_id
    WHERE b.session_id IS NOT NULL
      AND COALESCE(a.paid_date,a.submitted_date) >= CURRENT_DATE - INTERVAL '2 month')
    
,tradedoubler_campaigns_and_orders AS (
    SELECT order_id, 
        click_id,
        store_country,
        session_id,
        event_time,
        marketing_campaign
    FROM rank_td_clicks_and_orders
    WHERE rn = 1 
      AND order_id IS NOT NULL)

,everflow_campaigns AS (
    SELECT DISTINCT 
      a.session_id,
      a.marketing_campaign,
      b.order_id,
      ROW_NUMBER() OVER (PARTITION BY b.order_id ORDER BY a.page_view_start DESC) AS rn
    FROM traffic.page_views a
             LEFT JOIN traffic.session_order_mapping b ON a.session_id = b.session_id
    WHERE LOWER(a.marketing_source) = 'everflow')

,base_data AS (
SELECT DISTINCT
    event_time AS click_date
    ,store_country AS affiliate_country
    ,order_id
    ,marketing_campaign AS affiliate
    ,'Tradedoubler'::TEXT AS affiliate_network
    ,click_id
FROM tradedoubler_campaigns_and_orders

UNION ALL

SELECT DISTINCT
--DAISYCON REPORTS 2 HOURS AHAED (COMPARING SUBMITTED DATES) SO WE NORMALIZE THIS WITH BELOW
  DATEADD('HOUR', -2, date_click) AS click_date
  ,REPLACE(ip_country_name,'"','') AS affiliate_country
  ,transaction_id AS order_id
  ,media_name  AS affiliate
  ,'Daisycon'::TEXT AS affiliate_network
  ,NULL AS click_id
FROM marketing.affiliate_daisycon_submitted_orders

UNION ALL

SELECT DISTINCT
  a.event_date AS click_date
  ,b.store_country AS affiliate_country
  ,a.order_id
  ,a.publisher_name AS affiliate
  ,'CJ'::TEXT AS affiliate_network
  ,NULL AS click_id
FROM marketing.affiliate_cj_submitted_orders a
  LEFT JOIN master.order b ON a.order_id = b.order_id

UNION ALL

SELECT DISTINCT
  a.click_time AS click_date
  ,a.country AS affiliate_country
  ,a.order_id
  ,b.marketing_campaign AS affiliate
  ,'Everflow'::TEXT AS affiliate_network
  ,NULL AS click_id
FROM marketing.affiliate_everflow_submitted_orders a
  LEFT JOIN everflow_campaigns b ON a.order_id = b.order_id AND b.rn = 1
)

,commission_validation AS (
SELECT 
  *
 /*WE INVALIDATE COMMISSIONS FOR ORDERS APPEARING MULTIPLE TIMES WITH FOLLOWING RULE:
  * - BETWEEN TRADEDOUBLER AND DAISYCON BASED ON THE CLICK DATE (LAST CLICK WINS)*/ 
 ,ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(click_date,'1970-01-01') DESC) AS rx
FROM base_data
WHERE order_id IS NOT NULL
)

, exclusion_list AS (
/*WE EXCLUDE THE ORDERS:
 * - THAT WERE APPROVED BY MARKETING TEAM MANUALLY BEFORE
 * - THAT WERE EVALUATED BY US BEFORE*/
SELECT DISTINCT order_id 
FROM marketing.affiliate_validated_orders
WHERE COALESCE(paid_date::DATE, submitted_date::DATE, created_date::DATE,'9999-01-01') < DATE_ADD('day', -15, current_date) 

UNION 

SELECT DISTINCT order_id 
FROM marketing.marketing_cost_daily_affiliate_order
WHERE "date"::DATE < DATE_ADD('day', -15, current_date)
)

SELECT DISTINCT
 c.affiliate_network
 ,c.affiliate
 ,c.order_id
 ,o.new_recurring
 ,o.status AS order_status
 ,c.affiliate_country
 ,o.store_country AS order_country 
 ,c.click_date 
 ,o.created_date
 ,o.submitted_date
 ,o.paid_date
 ,CASE
   WHEN o.store_country = 'United States'
    THEN 'USD'
   WHEN o.store_country IN ('Germany', 'Austria', 'Spain', 'Netherlands')
    THEN 'EUR'
  END currency
 ,COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1) AS exchange_rate 
 ,CASE
   WHEN o.status = 'PAID' AND c.rx = 1 THEN 'APPROVED'
   WHEN o.status NOT IN ('DECLINED','CANCELLED','FAILED FIRST PAYMENT') AND c.rx = 1 THEN 'PENDING'
   ELSE 'DECLINED'
  END commission_approval
 ,COALESCE(ma.commission_type, ma2.commission_type) AS _commission_type
 ,COALESCE(ma.commission_amount, ma2.commission_amount)::DOUBLE PRECISION AS _commission_amount 
 ,COALESCE(ma.affiliate_network_fee_rate, ma2.affiliate_network_fee_rate)::DOUBLE PRECISION AS _affiliate_network_fee_rate
 ,CASE WHEN LOWER(c.affiliate_network) = 'tradedoubler' AND o.created_date::DATE >= '2023-03-01' 
  THEN ROUND(CASE
   WHEN _commission_type = 'ABSOLUT'
    THEN _commission_amount  
   WHEN _commission_type = 'PERCENTAGE'
  ELSE ROUND(CASE
   WHEN _commission_type = 'ABSOLUT'
    THEN _commission_amount  
   WHEN _commission_type = 'PERCENTAGE'
 ,c.click_id
FROM commission_validation c
  LEFT JOIN exclusion_list el
    ON c.order_id = el.order_id
  LEFT JOIN master.ORDER o
    ON c.order_id = o.order_id
  LEFT JOIN master.subscription s 
    ON o.order_id = s.order_id
  LEFT JOIN staging_google_sheet.affliates_commission_mapping ma
    ON LOWER(c.affiliate) = LOWER(ma.affiliate)
    AND LOWER(c.affiliate_network) = LOWER(ma.affiliate_network) 
    AND o.new_recurring = ma.new_recurring
    AND o.submitted_date::DATE BETWEEN ma.valid_from AND ma.valid_until 
  LEFT JOIN staging_google_sheet.affliates_commission_mapping ma2
    ON ma.affiliate_network IS NULL
    AND ma2.affiliate IS NULL
    AND LOWER(c.affiliate_network) = LOWER(ma2.affiliate_network) 
    AND o.new_recurring = ma2.new_recurring
    AND o.submitted_date::DATE BETWEEN ma2.valid_from AND ma2.valid_until  
  LEFT JOIN trans_dev.daily_exchange_rate exc
    ON o.submitted_date::DATE = exc.date_ 
    AND CASE 
      WHEN o.store_country = 'United States'
       THEN 'USD'
     END = exc.currency
  LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
    ON CASE 
      WHEN o.store_country = 'United States'
       THEN 'USD'
     END = exc.currency   
WHERE TRUE
  AND el.order_id IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,o.voucher_discount,c.click_id
WITH NO SCHEMA BINDING
;
