CREATE TEMP TABLE session_marketing_mapping_tmp AS 
WITH 
find_utms AS (
SELECT 
  a.session_id, 
  a.page_view_in_session_index,
  a.marketing_medium,
  a.marketing_source,
  a.marketing_campaign,
  a.marketing_content,
  COALESCE(NULLIF(a.marketing_medium, ''),'n/a') || COALESCE(a.marketing_source,'n/a') AS medium_source_map,
  ROW_NUMBER() OVER (PARTITION BY a.session_id ORDER BY a.page_view_in_session_index) AS rn
FROM web.page_views_snowplow a
  LEFT JOIN staging_airbyte_bi.affiliate_excluded_publishers b ON LOWER(a.marketing_campaign) = LOWER(b.publisher)
WHERE medium_source_map != 'n/an/a'
  AND DATE(a.page_view_start) >= CURRENT_DATE - 15
  AND b.publisher IS NULL
),
a AS (
SELECT
   pv.session_id
  ,snowplow_user_id
  ,page_view_start
  ,CASE 
    WHEN b.publisher IS NULL 
      THEN LOWER(COALESCE(NULLIF(pv.marketing_medium, ''),fu.marketing_medium, referer_medium, marketing_network))
    ELSE fu.marketing_medium END AS marketing_medium
  ,CASE 
    WHEN b.publisher IS NULL 
      THEN REPLACE(LOWER(COALESCE(pv.marketing_source, fu.marketing_source, referer_source)),'criteoacquisition','criteo')
    ELSE fu.marketing_source END AS marketing_source
  ,CASE 
    WHEN b.publisher IS NULL 
      THEN COALESCE(referer_term, marketing_term) 
    ELSE NULL END AS marketing_term
  ,CASE 
    WHEN b.publisher IS NULL
      THEN LOWER(COALESCE(pv.marketing_content, fu.marketing_content))
    ELSE fu.marketing_content END AS marketing_content
  ,LOWER(referer_url) AS referer_url
  ,LOWER(page_url) AS page_url
  ,page_type AS first_page_type
  ,CASE 
    WHEN b.publisher IS NULL THEN 
      CASE
        WHEN LOWER(COALESCE(pv.marketing_campaign,fu.marketing_campaign)) LIKE ('%sitelink%')
          THEN REPLACE(REPLACE(REPLACE(LOWER(SPLIT_PART(COALESCE(pv.marketing_campaign::TEXT,fu.marketing_campaign),'&',4)),'utm_campaign=',''),'%7c','|'),'-','|')
        WHEN LOWER(COALESCE(pv.marketing_campaign,fu.marketing_campaign)) LIKE ('%&%')
          THEN LOWER(SPLIT_PART(COALESCE(pv.marketing_campaign::TEXT,fu.marketing_campaign),'&',1))
        ELSE LOWER(COALESCE(pv.marketing_campaign,fu.marketing_campaign))  
      END
    ELSE fu.marketing_campaign
   END AS marketing_campaign
  ,referer_source 
FROM web.page_views_snowplow pv
   LEFT JOIN find_utms fu ON pv.session_id = fu.session_id AND fu.rn = 1
   LEFT JOIN staging_airbyte_bi.affiliate_excluded_publishers b ON LOWER(pv.marketing_campaign) = LOWER(b.publisher) 
WHERE pv.page_view_in_session_index = 1
  AND DATE(pv.page_view_start) >= CURRENT_DATE - 15
)
,channel_definitions AS (
SELECT DISTINCT 
  session_id
 ,snowplow_user_id
 ,page_view_start
 ,marketing_medium
 ,marketing_source
 ,marketing_term
 ,marketing_campaign
 ,referer_url
 ,CASE
   WHEN marketing_medium = 'direct'
     AND marketing_source IS NULL
     AND marketing_campaign IS NULL
     AND marketing_term IS NULL
    THEN TRUE
   ELSE FALSE
 END AS is_direct
 ,CASE
  WHEN marketing_medium = 'internal'
   THEN TRUE
  ELSE FALSE
 END AS is_internal
 ,CASE
   WHEN marketing_medium = 'g-pays'
    THEN TRUE
   ELSE FALSE
 END AS is_retail
 ,CASE
   WHEN marketing_medium IN (
       'branding_partnerships' 
       ,'edu_partnerships' 
       ,'add-on_partnerships' 
       ,'mno_integration'
       ,'strategic_partnerships'
       ,'growth_partnerships'
       )
    THEN TRUE
   ELSE FALSE
  END AS is_partnerships
 ,CASE
   WHEN marketing_medium ='podcasts'
    AND marketing_source = 'radio'
    THEN TRUE
   ELSE FALSE
  END AS is_offline_partnerships
 ,CASE
   WHEN marketing_medium IN ('affiliate', 'affiliates')
    THEN TRUE
   WHEN referer_url LIKE '%srvtrck.com%' 
    THEN TRUE
   WHEN referer_url LIKE '%tradedoubler.com%' 
    THEN TRUE       
  ELSE FALSE
 END AS is_affiliate
 ,CASE
   WHEN marketing_medium = 'display' 
    THEN TRUE
   ELSE FALSE
 END AS is_display
 ,CASE
   WHEN page_url LIKE '%/join%' OR page_url LIKE '%/referred%'
    THEN TRUE
   ELSE FALSE
  END AS is_refer_a_friend
,CASE
  WHEN marketing_source = 'braze'
    AND marketing_medium IN ('email', 'in_app', 'push_notification','sms')
   THEN TRUE
  ELSE FALSE
 END AS is_crm
,CASE
  WHEN marketing_medium = 'cpc'
    AND marketing_source IN ('facebook','instagram','linkedin','twitter','tiktok','snapchat')
   THEN TRUE
  ELSE FALSE
 END AS is_paid_social
,CASE
  WHEN marketing_medium = 'shopping'
   THEN TRUE
  ELSE FALSE
 END AS is_shopping
,CASE
  WHEN marketing_medium = 'social'
   THEN TRUE
 ELSE FALSE
END AS is_organic_social
,CASE
  WHEN marketing_medium = 'cpc'
    AND marketing_source IN ('google', 'bing', 'apple')
   THEN
    CASE
      WHEN marketing_content = 'brand' THEN TRUE
      WHEN marketing_content = 'non-brand' THEN FALSE
      WHEN b.brand_non_brand = 'brand' THEN TRUE
      WHEN b.brand_non_brand = 'non-brand' THEN FALSE
      WHEN marketing_campaign ILIKE '%brand%'  THEN TRUE
      WHEN marketing_campaign ILIKE '%trademark%' THEN TRUE
    ELSE FALSE END
  ELSE FALSE
 END AS is_paid_search_brand
,CASE
  WHEN marketing_medium = 'cpc'
    AND marketing_source IN ('google', 'bing', 'apple')
   THEN
    CASE
      WHEN marketing_content = 'non-brand' THEN TRUE
      WHEN marketing_content = 'brand' THEN FALSE
      WHEN b.brand_non_brand = 'non-brand' THEN TRUE
      WHEN b.brand_non_brand = 'brand' THEN FALSE
      WHEN marketing_campaign ILIKE '%brand%' THEN FALSE
      WHEN marketing_campaign ILIKE '%trademark%' THEN FALSE
    ELSE TRUE END
  ELSE FALSE
 END AS is_paid_search_non_brand
,CASE
  WHEN marketing_medium = 'influencers' 
   THEN TRUE
  ELSE FALSE
 END AS is_influencers
,CASE
  WHEN marketing_medium = 'search'
   THEN TRUE
  ELSE FALSE
 END AS is_organic_search
,CASE
  WHEN marketing_source = 'share' OR marketing_medium = 'referral' --OR a.referer_url IS NOT NULL 
   THEN TRUE
  ELSE FALSE
 END is_referral
FROM a 
LEFT JOIN marketing.campaigns_brand_non_brand b on a.marketing_campaign=b.campaign_name

)
,marketing_dimensions AS (
SELECT DISTINCT 
  session_id
 ,snowplow_user_id
 ,page_view_start
 ,is_internal
 ,is_display
 ,is_paid_search_brand
 ,is_paid_search_non_brand
 ,is_paid_social
 ,is_shopping
 ,is_refer_a_friend
 ,is_influencers
 ,is_affiliate
 ,is_partnerships
 ,is_offline_partnerships
 ,is_retail
 ,is_organic_social
 ,is_direct
 ,is_organic_search
 ,is_crm
 ,is_referral
 ,COALESCE(marketing_term,'n/a') AS marketing_term
 ,COALESCE(marketing_campaign,'n/a') AS marketing_campaign
 ,referer_url
,CASE 
  WHEN is_display      
   THEN TRUE
  WHEN is_paid_search_brand
   THEN TRUE
  WHEN is_paid_search_non_brand
   THEN TRUE
  WHEN is_paid_social                                 
   THEN TRUE
  WHEN is_shopping                           
   THEN TRUE
  WHEN is_partnerships 
   THEN TRUE
  WHEN is_offline_partnerships 
   THEN TRUE
  WHEN is_affiliate 
   THEN TRUE
  WHEN is_influencers 
   THEN TRUE
  WHEN is_retail 
   THEN TRUE
  ELSE FALSE
 END AS is_paid
 ,CASE 
  WHEN is_display
   THEN 'Display'
  WHEN is_paid_search_brand
   THEN 'Paid Search Brand'
  WHEN is_paid_search_non_brand
   THEN 'Paid Search Non Brand'
  WHEN is_paid_social 
   THEN 'Paid Social'
  WHEN is_shopping 
   THEN 'Shopping'
  WHEN is_crm 
   THEN 'CRM'
  WHEN is_partnerships 
   THEN 'Partnerships'
  WHEN is_offline_partnerships 
   THEN 'Offline Partnerships'
  WHEN is_affiliate 
   THEN 'Affiliates'
  WHEN is_influencers 
   THEN 'Influencers'
  WHEN is_organic_search
   THEN 'Organic Search'
  WHEN is_retail 
   THEN 'Retail'
  WHEN is_organic_social 
   THEN 'Organic Social'
  WHEN is_refer_a_friend 
   THEN 'Refer Friend'
  WHEN is_referral 
   THEN 'Referral'
  WHEN is_direct 
   THEN 'Direct'
  WHEN is_internal 
   THEN 'Internal'
  ELSE 'Others'
 END AS marketing_channel
 ,marketing_source
 ,marketing_medium
 ,CASE
   WHEN marketing_channel = 'Internal' 
    THEN LAG(CASE WHEN marketing_channel = 'Internal' THEN NULL ELSE session_id END) 
     IGNORE NULLS OVER (PARTITION BY snowplow_user_id ORDER BY page_view_start, session_id)
   END AS last_non_internal_session_id
FROM channel_definitions
)

SELECT DISTINCT 
  m1.session_id,
  m1.page_view_start,
  COALESCE(m2.marketing_channel, m1.marketing_channel) AS marketing_channel,
  COALESCE(m2.marketing_medium, m1.marketing_medium) AS marketing_medium ,
  COALESCE(m2.marketing_source, m1.marketing_source) AS marketing_source,
  CASE WHEN COALESCE(m2.marketing_channel, m1.marketing_channel) IN ('Organic Search', 'CRM') THEN FALSE ELSE COALESCE(m2.is_paid, m1.is_paid) END AS is_paid,
  COALESCE(m2.marketing_campaign, m1.marketing_campaign) AS marketing_campaign,
  COALESCE(m2.marketing_term, m1.marketing_term) AS marketing_term,
  m1.referer_url,
  m1.is_internal,
  m1.is_display,
  m1.is_paid_search_brand,
  m1.is_paid_search_non_brand,
  m1.is_paid_social,
  m1.is_shopping,
  m1.is_refer_a_friend,
  m1.is_influencers,
  m1.is_affiliate,
  m1.is_partnerships,
  m1.is_offline_partnerships,
  m1.is_retail,
  m1.is_organic_social,
  m1.is_direct,
  m1.is_organic_search,
  m1.is_crm,
  m1.is_referral
FROM marketing_dimensions m1
  LEFT JOIN marketing_dimensions m2
    ON m1.last_non_internal_session_id = m2.session_id
WHERE m1.page_view_start::DATE >= CURRENT_DATE - 10
;

BEGIN TRANSACTION; 
DELETE FROM web.session_marketing_mapping_snowplow
USING session_marketing_mapping_tmp tmp
WHERE session_marketing_mapping_snowplow.session_id = tmp.session_id
;

INSERT INTO web.session_marketing_mapping_snowplow
SELECT *
FROM session_marketing_mapping_tmp
;

END TRANSACTION;

DROP TABLE session_marketing_mapping_tmp;
