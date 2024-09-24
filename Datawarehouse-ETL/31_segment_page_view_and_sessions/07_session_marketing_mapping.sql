DROP TABLE IF EXISTS segment.session_marketing_mapping_web;
CREATE TABLE segment.session_marketing_mapping_web AS
WITH session_unique AS (
    SELECT
        a.session_id,
        a.event_time AS session_start,
        a.anonymous_id,
        b.customer_id,
        ROW_NUMBER() OVER(PARTITION BY a.session_id ORDER BY event_time) AS rn
    FROM segment.all_events a
        LEFT JOIN segment.customer_mapping_web b USING (event_id)
    WHERE a.platform = 'web'
),

manipulate_context_utms AS (
    SELECT
        session_id,
        LOWER(marketing_content) AS _marketing_content,
        LOWER(marketing_medium) AS _marketing_medium,
        LOWER(marketing_campaign) AS _marketing_campaign,
        LOWER(marketing_source) AS _marketing_source,
        LOWER(marketing_term) AS _marketing_term,
        COALESCE(marketing_medium,'-')||COALESCE(marketing_source,'-') AS exclude_refference, --in order to find first row with any utm parameter so we can get the first event_order later
        ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_time) AS event_order,
        page_referrer,
        page_url
    FROM segment.all_events
    WHERE platform = 'web'
      AND exclude_refference != '--'
),

manipulate_traits_utms AS (
    SELECT
        session_id,
        LOWER(traits_content) AS traits_content,
        LOWER(traits_medium) AS traits_medium,
        LOWER(traits_campaign) AS traits_campaign,
        LOWER(traits_source) AS traits_source,
        LOWER(traits_term) AS traits_term,
        COALESCE(traits_medium,'-')||COALESCE(traits_source,'-') AS exclude_refference, --in order to find first row with any utm parameter so we can get the first event_order later
        ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_time) AS traits_event_order
    FROM segment.all_events
    WHERE platform = 'web'
      AND exclude_refference != '--'
)

SELECT
    a.session_id,
    a.anonymous_id,
    a.customer_id,
    a.session_start,
    CASE WHEN b._marketing_medium IS NOT NULL OR b._marketing_source IS NOT NULL THEN b._marketing_content ELSE c.traits_content END AS marketing_content,
    CASE WHEN b._marketing_medium IS NOT NULL OR b._marketing_source IS NOT NULL THEN b._marketing_medium ELSE c.traits_medium END AS marketing_medium,
    CASE WHEN b._marketing_medium IS NOT NULL OR b._marketing_source IS NOT NULL THEN b._marketing_campaign ELSE c.traits_campaign END AS marketing_campaign,
    CASE WHEN b._marketing_medium IS NOT NULL OR b._marketing_source IS NOT NULL THEN b._marketing_source ELSE c.traits_source END AS marketing_source,
    CASE WHEN b._marketing_medium IS NOT NULL OR b._marketing_source IS NOT NULL THEN b._marketing_term ELSE c.traits_term END AS marketing_term,
    b.page_referrer,
    CASE        
            OR (b.page_url ilike '%/join%' OR b.page_url ILIKE '%/referred%')
            THEN 'Refer Friend'
        WHEN marketing_medium = 'internal'
            THEN 'Internal'
        WHEN marketing_medium = 'sponsorships'
            THEN 'Sponsorships'
        WHEN marketing_medium = 'g-pays'
            THEN 'Retail'
        WHEN marketing_medium IN (
                                  'branding_partnerships'
            ,'edu_partnerships'
            ,'add-on_partnerships'
            ,'mno_integration'
            ,'strategic_partnerships'
            ,'growth_partnerships'
            )
            THEN 'Partnerships'
        WHEN marketing_medium ='podcasts'
            AND marketing_source = 'radio'
            THEN 'Offline Partnerships'
        WHEN marketing_medium ='podcast' 
            THEN 'Podcasts'
        WHEN marketing_medium IN ('affiliate', 'affiliates')
            OR b.page_referrer ilike '%srvtrck.com%'
            OR b.page_url ilike '%tradedoubler.com%'
            THEN 'Affiliates'
        WHEN marketing_medium = 'display' THEN
            CASE WHEN marketing_campaign ILIKE '%awareness%' OR
                      marketing_campaign ILIKE '%traffic%'
                 THEN 'Display Branding' 
            ELSE 'Display Performance' 
            END
        WHEN marketing_source = 'braze'
            AND marketing_medium IN ('email','push_notification','sms')
            THEN 'CRM'
        WHEN marketing_medium = 'cpc'
            AND marketing_source IN ('facebook','instagram','linkedin','twitter','tiktok','snapchat','reddit')
            THEN 
            CASE WHEN marketing_content ILIKE '%traffic%' AND a.session_start::DATE < '2023-05-01'
                    THEN 'Paid Social Branding' 
                 WHEN marketing_campaign ILIKE '%brand%' AND a.session_start::DATE >= '2023-05-01'
                    THEN 'Paid Social Branding' 
            ELSE 'Paid Social Performance' 
            END 
        WHEN marketing_medium = 'shopping'
            THEN 'Shopping'
        WHEN marketing_medium = 'social' THEN 'Organic Social'
        WHEN marketing_medium = 'organic'
            THEN CASE WHEN marketing_source ILIKE '%facebook%'
            OR marketing_source ILIKE '%igshopping%'
            OR marketing_source ILIKE '%instagram%'
            OR marketing_source ILIKE '%linkedin%'
            OR marketing_source ILIKE '%twitter%'
            OR marketing_source ILIKE '%tiktok%'
            OR marketing_source ILIKE '%snapchat%'
            OR marketing_source ILIKE '%youtube%'
            OR marketing_source ILIKE '%bild%'
            OR marketing_source ILIKE '%press%'
                          THEN 'Organic Social'
                      ELSE 'Organic Search'
            END
        WHEN marketing_medium = 'cpc' AND marketing_source IN ('google', 'bing', 'apple') THEN
            CASE
                WHEN marketing_content = 'brand' OR
                     marketing_campaign ILIKE '%brand%' OR
                     marketing_campaign ILIKE '%trademark%'
                    THEN 'Paid Search Brand'
                ELSE 'Paid Search Non Brand'
                END
        WHEN marketing_medium = 'influencers'
            THEN 'Influencers'
        WHEN marketing_source = 'share' OR marketing_medium = 'referral'
            THEN 'Referral'
        WHEN marketing_source  = 'direct' OR (marketing_medium IS NULL AND marketing_source IS NULL AND b.page_referrer IS NULL)
            THEN 'Direct'
        WHEN marketing_source = 'braze' AND marketing_medium = 'in_app' THEN 
            CASE 
                WHEN a.session_start::DATE < '2023-05-23' THEN 'CRM' 
                ELSE 'Direct' 
            END
        ELSE 'Others'
        END AS marketing_channel
FROM session_unique a
         LEFT JOIN manipulate_context_utms b ON a.session_id = b.session_id AND b.event_order = 1
         LEFT JOIN manipulate_traits_utms c ON a.session_id = c.session_id AND c.traits_event_order = 1
WHERE a.rn = 1;

CREATE TEMP TABLE tmp_facebook_campaign_mapping AS 
WITH 
    traffic_campaign_names AS (
        SELECT DISTINCT marketing_campaign, marketing_content
        FROM segment.session_marketing_mapping_web
        WHERE marketing_channel ILIKE '%Paid Social%' 
            AND marketing_source = 'facebook'
),

    cost_campaigns as (
        SELECT LOWER(campaign_name) AS marketing_campaign,
               MIN(date) AS campaign_start
        FROM marketing.marketing_cost_daily_facebook
        GROUP BY 1
    ),

    find_real_campaign AS (
        SELECT a.*, 
            COALESCE(b.marketing_campaign,c.marketing_campaign) AS marketing_campaign_cost,
        CASE WHEN marketing_campaign_cost ILIKE '%brand%' and b.campaign_start >= '2023-05-01'
            THEN 'Paid Social Branding'
            WHEN marketing_campaign_cost ILIKE '%traffic%' and b.campaign_start < '2023-05-01'
            THEN 'Paid Social Branding' 
        ELSE 'Paid Social Performance' 
        END AS new_channel
        FROM traffic_campaign_names a
        LEFT JOIN cost_campaigns b ON a.marketing_campaign = b.marketing_campaign
        LEFT JOIN cost_campaigns c ON a.marketing_content = c.marketing_campaign
    )

    SELECT DISTINCT marketing_campaign, new_channel
    FROM find_real_campaign;

UPDATE segment.session_marketing_mapping_web AS s
SET marketing_channel = t.new_channel
FROM tmp_facebook_campaign_mapping AS t
WHERE s.marketing_campaign = t.marketing_campaign
    AND s.marketing_channel IN ('Paid Social Branding','Paid Social Performance');


GRANT SELECT ON segment.session_marketing_mapping_web TO hams;
