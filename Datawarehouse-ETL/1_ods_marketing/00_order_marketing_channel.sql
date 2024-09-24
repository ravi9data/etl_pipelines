drop table if exists ods_production.order_marketing_channel;
create table ods_production.order_marketing_channel AS
with ga as (
    select distinct ga.*,
                    o.submitted_date,
                    o.store_type,
                    rank() over (partition by ga.dimensions order by ga.date) as rank_touchpoints,
                    count(*) over (partition by ga.dimensions) as total_touchpoints
    from stg_external_apis.ga_order_campaign ga
             inner join ods_production.order o on o.order_id=ga.dimensions
    where date <= submitted_date)
   ,ga2 as (
    select distinct
        dimensions as order_id,
        store_type,
        channelgrouping as marketing_channel,
        campaign as marketing_campaign,
        source as marketing_source,
        case when devicecategory='tablet' then 'Tablet'
             when devicecategory='desktop' then 'Computer'
             when devicecategory='mobile' then 'Mobile'
             else 'n/a'
            end
            as devicecategory
    from ga
    where rank_touchpoints = total_touchpoints
)
   , influencers_voucher AS (
    SELECT
        lower(voucher) AS voucher,
        MAX(influencer_name) AS influencer_name
    FROM staging.influencers_fixed_costs
    GROUP BY 1
)
   ,podcasts_voucher AS (
    select 
        lower(voucher) AS voucher,
        max(lower(utm_source)) AS utm_source,
        max(lower(utm_campaign)) AS utm_campaign,
        max(lower(utm_medium)) as utm_medium
    from staging.podcasts_voucher_tracking
    group by 1
)
      ,sponsorships_voucher AS (
    select 
        lower(voucher) AS voucher,
        max(lower(utm_source)) AS utm_source,
        max(lower(utm_campaign)) AS utm_campaign,
        max(lower(utm_medium)) as utm_medium
    from staging.sponsorships_vouchers
    group by 1
)
   ,partnerships_voucher AS (
    SELECT
        lower(voucher_prefix_code) AS voucher_prefix_code,
        MAX(partnership_name) AS partnership_name,
        MAX(mkt_campaign) AS mkt_campaign
    FROM stg_external_apis.partnerships_vouchers
    GROUP BY 1
)
, prep AS (
	SELECT DISTINCT
	    so.order_id as order_id,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online'
	             THEN 'Retail'
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'Refer Friend'
	         WHEN pov.voucher IS NOT NULL THEN 'Podcasts'
	         WHEN spv.voucher IS NOT NULL THEN 'Sponsorships'
	         WHEN v.voucher IS NOT NULL THEN 'Influencers'
	         WHEN pv.partnership_name IS NOT NULL THEN 'Partnerships'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) = 'Affiliates'
	             then 'Affiliates'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Waiting list offers','Email','CRM')
	             then 'CRM'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) = 'Direct'
	             then 'Direct'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) = 'Paid Search'
	             then
	             case
	                 when coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) ilike '%brand%'
	                     or coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) ilike '%trademark%'
	                     then 'Paid Search Brand'
	                 else 'Paid Search Non Brand'
	                 end
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Other','(Other)')
	             then 'Other'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Retargeting','Display')
	         then
	             case
	                 when coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) ilike '%awareness%'
	                     or coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) ilike '%traffic%'
	                     then 'Display Branding'
	                 else 'Display Performance'
	                 end
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Paid Social','Paid Content')
	             then
	             case
	                 when coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) ilike '%brand%'
	                     then 'Paid Social Branding'
	                 else 'Paid Social Performance'
	                 end
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('ReferralRock','Refer Friend')
	             then 'Refer Friend'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Retail','Direct Partnerships')
	             then 'Retail'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Referral','Referrals')
	             then 'Referral'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) = 'Partnerships'
	             then 'Partnerships'
	         when coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel) in ('Organic Social', 'Social')
	             then 'Organic Social'
	         else COALESCE(coalesce(oc.last_touchpoint_excl_direct,ga2.marketing_channel),'n/a')
	        end as marketing_channel,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online'
	             THEN 'n/a'
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'new_referral_30'
	         WHEN pov.voucher IS NOT NULL THEN pov.utm_source
	         WHEN spv.voucher IS NOT NULL THEN spv.utm_source
	         WHEN v.voucher IS NOT NULL THEN 'n/a'
	         WHEN pv.partnership_name IS NOT NULL THEN 'n/a'
	         ELSE coalesce(oc.last_touchpoint_excl_direct_mkt_source,ga2.marketing_source) END as marketing_source,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online' THEN s2.store_label
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'refer_friend'
	         WHEN pov.voucher IS NOT NULL THEN pov.utm_medium
	         WHEN spv.voucher IS NOT NULL THEN spv.utm_medium
	         WHEN v.voucher IS NOT NULL THEN 'n/a'
	         WHEN pv.partnership_name IS NOT NULL THEN 'n/a'
	         ELSE oc.last_touchpoint_excl_direct_mkt_medium END as marketing_medium,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online' THEN 'n/a'
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'n/a'
	         WHEN v.voucher IS NOT NULL THEN v.influencer_name
	         WHEN pov.voucher IS NOT NULL THEN pov.utm_campaign
	         WHEN spv.voucher IS NOT NULL THEN spv.utm_campaign
	         WHEN pv.partnership_name IS NOT NULL THEN COALESCE(pv.mkt_campaign, 'n/a')
	         ELSE coalesce(oc.last_touchpoint_excl_direct_marketing_campaign, ga2.marketing_campaign) END AS marketing_campaign,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online' THEN 'n/a'
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'n/a'
	         WHEN v.voucher IS NOT NULL THEN 'n/a'
	         WHEN pov.voucher IS NOT NULL THEN 'n/a'
	         WHEN spv.voucher IS NOT NULL THEN 'n/a'
	         WHEN pv.partnership_name IS NOT NULL THEN 'n/a'
	         ELSE oc.last_touchpoint_excl_direct_marketing_content END AS marketing_content,
	    CASE WHEN s2.store_short
	                  IN ('Partners Online',
	                      'Partners Offline') AND s2.store_label != 'Bob AT online' THEN 'n/a'
	         WHEN LEFT(so.voucher_code,3) = 'RF_' THEN 'n/a'
	         WHEN v.voucher IS NOT NULL THEN 'n/a'
	         WHEN pov.voucher IS NOT NULL THEN 'n/a'
	         WHEN spv.voucher IS NOT NULL THEN 'n/a'
	         WHEN pv.partnership_name IS NOT NULL THEN 'n/a'
	         ELSE oc.last_touchpoint_excl_direct_marketing_term END AS marketing_term,
	    coalesce((case when s2.store_type = 'offline' then 'n/a' else oc.last_touchpoint_device_type end) ,ga2.devicecategory,'n/a') as devicecategory,
	    coalesce(os,'n/a') as os,
	    coalesce(browser,'n/a') as browser,
	    coalesce(oc.geo_cities,'n/a') as geo_cities,
	    coalesce(oc.no_of_geo_cities::INT,0) as no_of_geo_cities,
	    coalesce(
	            case
	                when coalesce(last_touchpoint_checkout_flow,'')=''
	                    then customer_journey_checkout_flow
	                else last_touchpoint_checkout_flow
	                end
	        ,'n/a') as checkout_flow
	        ,coalesce(oc.last_touchpoint_referer_url,'n/a') as referer_url
	        ,is_paid
	from ods_production."order"  so
	         left join traffic.order_conversions oc
	                   on oc.order_id=so.order_id
	         left join ods_production.store s2
	                   on s2.id = so.store_id
	         left join ga2
	                   on ga2.order_id=so.order_id
	         left join (
	    select distinct
	        marketing_channel as channel, is_paid
	    from traffic.sessions s
		where session_start >= '2023-05-01'
	) p
	                   on p.channel=oc.last_touchpoint_excl_direct
	         LEFT JOIN influencers_voucher v
	                   ON lower(so.voucher_code) = lower(v.voucher)
	         LEFT JOIN podcasts_voucher pov 
	                   ON lower(so.voucher_code) = lower(pov.voucher)
	         LEFT JOIN sponsorships_voucher spv 
	                   ON lower(so.voucher_code) = lower(spv.voucher)
	         LEFT JOIN partnerships_voucher pv
	                   ON (CASE WHEN so.voucher_code LIKE 'BIDROOM%' THEN so.voucher_code + so.store_country
	                            WHEN so.voucher_code LIKE 'MILITARY%' THEN 'MILITARY'
	                            WHEN so.voucher_code ILIKE 'n26-%' --TO CONNECT an OLD n26 voucher
	                                AND so.voucher_code NOT ILIKE 'n26-esp%'
	                                AND so.voucher_code NOT ILIKE 'n26-nld%' THEN 'n26-oldvoucher'
	                            WHEN so.voucher_code ILIKE 'iam%'
	                            	 AND so.voucher_code NOT ILIKE 'iamat%'
	                            	 AND so.voucher_code NOT ILIKE 'iamde%' THEN 'iam-oldvoucher'
	                            ELSE so.voucher_code END)
	                       ILIKE
	                      (CASE
	                           WHEN lower(pv.voucher_prefix_code) = 'n26-' THEN 'n26-oldvoucher'
	                           WHEN lower(pv.voucher_prefix_code) = 'iam' THEN 'iam-oldvoucher'
	                           WHEN pv.partnership_name = 'N26 Spain' THEN '%'+voucher_prefix_code+'%'
	                           WHEN pv.partnership_name = 'N26 Netherlands' THEN '%'+voucher_prefix_code+'%'
	                           WHEN pv.partnership_name  LIKE 'Bidroom%' THEN '%'+voucher_prefix_code+split_part(pv.partnership_name,' ',2)+'%'
	                           ELSE voucher_prefix_code+'%' END)
)
SELECT 
	order_id, 
	max(marketing_channel) AS marketing_channel, 
	max(marketing_source) AS marketing_source,
	max(marketing_medium) AS marketing_medium,
	max(marketing_campaign) AS marketing_campaign,
	max(marketing_content) AS marketing_content,
	max(marketing_term) AS marketing_term,
	max(devicecategory) AS devicecategory,
	max(os) AS os,
	max(browser) AS browser,
	max(geo_cities) AS geo_cities,
	max(no_of_geo_cities) AS no_of_geo_cities,
	max(checkout_flow) AS checkout_flow,
	max(referer_url) AS referer_url,
	is_paid
FROM prep 
GROUP BY 1,15
;
                   
GRANT SELECT ON ods_production.order_marketing_channel TO tableau;
GRANT SELECT ON ods_production.order_marketing_channel TO redash_growth;
