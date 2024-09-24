BEGIN TRANSACTION;

DROP TABLE IF EXISTS marketing_cost_daily_base_data_tmp;
CREATE TEMP TABLE marketing_cost_daily_base_data_tmp
AS
SELECT
    "date"
     ,'eBay Kleinanzeigen'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,'n/a' AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_ebay_kleinanzeigen --old table data used up untill '2023-10-01', without campaign names
WHERE "date" < '2023-10-01'
GROUP BY 1,2,8,13

UNION ALL

SELECT
    "date"
     ,'eBay Kleinanzeigen'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,CASE WHEN campaign = 'Default Campaign' THEN 'default' ELSE LOWER(campaign) END AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_ebay_kleinanzeigen_campaign --new data from '2023-10-01' campaign name available
WHERE "date" >= '2023-10-01'
GROUP BY 1,2,8,13

UNION ALL

SELECT
    "date"
     ,'Marktplaats'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL AS impressions
     ,NULL AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_marktplaats_cost --no campaigns here, only date and costs
GROUP BY 1,2,13

UNION ALL 

SELECT
    "date"
     ,'Kleinanzeigen'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,'Germany' AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL AS impressions
     ,NULL AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_display_kleinanzeigen --no campaigns here, only date and costs
WHERE "date" <= CURRENT_DATE  
GROUP BY 1,2,13

UNION ALL 

SELECT
    "date"
     ,'Criteo'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,'Germany' AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL AS impressions
     ,NULL AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_criteo --old table 2019-2021 no campaign names
GROUP BY 1,2,13

UNION ALL 

SELECT
    "date"
     ,'Criteo'::text AS channel
     ,account_name AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,CASE WHEN account_name = 'Grover DE' THEN 'Germany'
        WHEN account_name = 'Grover ES' THEN 'Spain' END AS country
     ,NULL AS customer_type
     ,COALESCE(campaign_name, 'n/a') AS campaign_name
     ,campaign_id
     ,NULL AS medium
     ,ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_daily_criteo --daily costs with campaign names
GROUP BY 1,2,3,6,8,9,11,13

UNION ALL

SELECT
    "date"
     ,'Milanuncios'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL AS impressions
     ,NULL AS clicks
     ,SUM(cost)::double precision AS total_spent_local_currency
     ,SUM(cost)::double precision AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_milanuncios_cost
GROUP BY 1,2,13

UNION ALL

SELECT
    "date"
     ,'Apple Search'::text AS channel
     ,account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,campaign_name
     ,campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,CASE
          WHEN campaign_name ILIKE '%test_%' THEN TRUE
          ELSE FALSE
      END AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(installs) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,NULL AS conversions
FROM marketing.marketing_cost_daily_apple 
GROUP BY 1,3,8,9

UNION ALL

SELECT
    "date"
     ,channel
     ,account
     ,advertising_channel_type
     ,campaign_group_name
     ,country
     ,customer_type
     ,campaign_name
     ,campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,is_test_and_learn
     ,brand_non_brand
     ,cash_percentage::DOUBLE PRECISION
     ,non_cash_percentage::DOUBLE PRECISION
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.v_google_cost_daily_with_invoice_credit --view created as we had some credits received from google so we proportionaly reduced costs.
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

UNION ALL

SELECT
    "date"
     ,'Linkedin'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,campaign_name
     ,campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_linkedin
GROUP BY 1,5,8,9

UNION ALL

SELECT
    "date"
     ,'Bing'::text AS channel
     ,account
     ,advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,campaign_name
     ,campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_bing
GROUP BY 1,3,4,8,9

UNION ALL

-- Paid Social starts here
SELECT
    "date"
     ,channel -- Facebook
     ,account
     ,advertising_channel_type
     ,campaign_group_name
     ,country
     ,customer_type
     ,campaign_name
     ,campaign_id
     ,medium
     ,ad_set_name
     ,ad_name
     ,is_test_and_learn
     ,brand_non_brand
     ,cash_percentage::DOUBLE PRECISION
     ,non_cash_percentage::DOUBLE PRECISION
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.v_facebook_cost_daily_with_invoice_credit --view created as we had some credits received from google so we proportionaly reduced costs.
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

UNION ALL

SELECT
    "date"
     ,'TikTok'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
--This campaigns are not being allocated because of wrong campaign namings.
--That's why I am adding customer type manually
     ,campaign_name
     ,campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,CASE
          WHEN campaign_name ILIKE '%test_%' THEN TRUE
          ELSE FALSE
    END AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_tiktok
GROUP BY 1,8,9,13

UNION ALL

SELECT
    "date"
     ,'Snapchat'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,CASE
          WHEN campaign_name ILIKE '%test_%' THEN TRUE
          ELSE FALSE
    END AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_snapchat mcds
GROUP BY 1,8,12

UNION ALL

SELECT
    "date"
     ,'Outbrain'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,NULL AS country
     ,NULL AS customer_type
     ,campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,CASE
          WHEN ("date" >= '2022-02-01' AND "date" <= '2022-02-28') AND campaign_name ILIKE '%B2B_%' THEN TRUE
          WHEN campaign_name ILIKE '%test_%' THEN TRUE
          ELSE FALSE
    END AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,SUM(impressions) AS impressions
     ,SUM(clicks) AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_eur) AS total_spent_eur
     ,SUM(conversions) AS conversions
FROM marketing.marketing_cost_daily_outbrain mcds
GROUP BY 1,8,12
-- Paid Social ends here

UNION ALL

SELECT
    s."date"
     ,'Organic Search'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,s.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,s.is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(s.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(s.total_spent_local_currency *
          COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_seo s --old table data for 2021
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON s.date::date = exc.date_
                       AND s.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON s.currency = exc_last.currency
GROUP BY 1,6,13

UNION ALL

SELECT
    ord.paid_date::date AS date
     ,'Vouchers'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,CASE
          WHEN ord.store_label = 'Grover - Netherlands online'
              THEN 'Netherlands'
          WHEN ord.store_label = 'Grover - Austria online'
              THEN 'Austria'
          WHEN ord.store_label = 'Grover - Spain online'
              THEN 'Spain'
          WHEN ord.store_commercial = 'B2B Germany'
              THEN 'Germany'
          WHEN ord.store_commercial = 'Grover Germany'
              THEN 'Germany'
          WHEN ord.store_label = 'Grover - United States online'
              THEN 'United States'
          ELSE 'Others'
    END AS country
     ,CASE
          WHEN ord.store_commercial LIKE '%B2B%'
              THEN 'B2B'
          ELSE 'B2C'
    END AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,COALESCE(SUM(ord.voucher_discount),0) AS total_spent_local_currency
     ,COALESCE(SUM(ord.voucher_discount *
                   COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM master.order ord
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON ord.paid_date::date = exc.date_
                       AND ord.store_label = 'Grover - United States online'
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON ord.store_label = 'Grover - United States online'
WHERE TRUE
   AND paid_orders >=1
   AND LEFT(voucher_code,3) != 'RF_'
   AND country != 'Others'
GROUP BY 1,6,7
HAVING SUM(voucher_discount) > 0

UNION ALL

SELECT
    week_date::date AS date
     ,'Influencers'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,country
     ,customer_type
     ,campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,1::float AS cash_percentage
     ,0::float AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))  AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_influencer_summary_2021_10_14 m --influencer old costs
         LEFT JOIN  trans_dev.daily_exchange_rate exc
                    ON m.week_date::date = exc.date_
                        AND m.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON m.currency = exc_last.currency
WHERE week_date::date <= '2022-05-29'
GROUP BY 1,6,7,8,15,16

UNION ALL

SELECT 
	  reporting_date::date AS date
	 ,'Influencers'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,country
     ,i.customer_type
     ,utm_campaign AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,1::float AS cash_percentage
     ,0::float AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(COALESCE(agency_fee::float,0) 
     	+ COALESCE(total_spent_local_currency::float,0)) AS total_spent_local_currency
     ,SUM(COALESCE(agency_fee::float,0) * COALESCE(exc2.exchange_rate_eur, exc_last2.exchange_rate_eur, 1)
     	+ COALESCE(total_spent_local_currency::float,0) 
     		* COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM staging.influencers_fixed_costs i --influencer fixed costs
LEFT JOIN  trans_dev.daily_exchange_rate exc
                    ON i.reporting_date::date = exc.date_
                        AND i.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON i.currency = exc_last.currency
LEFT JOIN  trans_dev.daily_exchange_rate exc2
                    ON i.reporting_date::date = exc2.date_
                        AND i.currency_agency_fee = exc2.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last2
                   ON i.currency_agency_fee = exc_last2.currency
WHERE reporting_date::date >= '2022-05-30'
GROUP BY 1,6,7,8,15,16

UNION ALL 

SELECT
    sp.paid_date::date AS date
     ,'Influencers'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,CASE
          WHEN ord.store_label = 'Grover - Netherlands online'
              THEN 'Netherlands'
          WHEN ord.store_label = 'Grover - Austria online'
              THEN 'Austria'
          WHEN ord.store_label = 'Grover - Spain online'
              THEN 'Spain'
          WHEN ord.store_commercial = 'B2B Germany'
              THEN 'Germany'
          WHEN ord.store_commercial = 'Grover Germany'
              THEN 'Germany'
          WHEN ord.store_label = 'Grover - United States online'
              THEN 'United States'
          ELSE 'Others'
    END AS country
     ,cc.customer_type
     ,cc.utm_campaign AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,0::float AS cash_percentage
     ,1::float AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,COALESCE(SUM(sp.amount_subscription),0) AS total_spent_local_currency
     ,COALESCE(SUM(sp.amount_subscription *
                   COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) 
                   AS total_spent_eur
     ,NULL::bigint AS conversions
FROM master.order ord
LEFT JOIN master.subscription_payment sp 
	ON ord.order_id = sp.order_id
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON sp.paid_date::date = exc.date_
                       AND sp.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON sp.currency = exc_last.currency
         INNER JOIN staging.influencers_fixed_costs cc 
				ON cc.lending_order_id = ord.order_id
WHERE sp.paid_date IS NOT NULL
  AND country != 'Others'
GROUP BY 1,6,7,8,15,16
HAVING SUM(sp.amount_subscription) > 0

UNION ALL

SELECT
    week_date::date AS date
     ,'Affiliates'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,country
     ,'B2C' AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(total_spent_local_currency) AS total_spent_local_currency
     ,SUM(total_spent_local_currency) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_affiliate_summary_2021_10_14
GROUP BY 1,6

UNION ALL

SELECT
    af.date::date AS date
     ,'Affiliates'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,af.country
     ,'B2C' AS customer_type
     ,lower(affiliate) AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(af.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(af.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_affiliate_order af
         LEFT JOIN  trans_dev.daily_exchange_rate exc
                    ON af.date::date = exc.date_
                        AND af.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON af.currency = exc_last.currency
WHERE af.date::date BETWEEN '2021-10-18' AND '2023-01-14' 
GROUP BY 1,6,8

UNION ALL

SELECT
    submitted_date::date AS date
     ,'Affiliates'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,order_country AS country
     ,'B2C' AS customer_type
     ,lower(affiliate) AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,NULL::bigint AS conversions
FROM marketing.affiliate_validated_orders af
WHERE submitted_date::date >= '2023-01-15'
    AND commission_approval = 'APPROVED'
GROUP BY 1,6,8

UNION ALL

SELECT
     COALESCE(af.date,ord.submitted_date)::DATE AS date
     ,'Affiliates'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,'Spain' AS country
     ,'B2B' AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(paid_commission) AS total_spent_local_currency  -- paid_commission is in EUR in this table and it is only for ES
     ,SUM(paid_commission) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_yakk_b2b_cost af
    LEFT JOIN master."order" ord
        ON af.order_id = ord.order_id
GROUP BY 1,2,6

UNION ALL

SELECT
    af.date::date AS date
     ,'Affiliates'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,af.country
     ,'B2C' AS customer_type
     ,lower(affiliate) AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(af.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(af.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_affiliate_fixed af
         LEFT JOIN  trans_dev.daily_exchange_rate exc
                    ON af.date::date = exc.date_
                        AND af.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON af.currency = exc_last.currency
WHERE af.date::date >= '2021-10-18'
GROUP BY 1,6,8

UNION ALL

SELECT
    pa.date::date AS date
     ,'Partnerships'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,pa.country
     ,NULL AS customer_type
     ,lower(campaign_name) AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,pa.is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(pa.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(pa.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_partnership  pa
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON pa.date::date = exc.date_
                       AND pa.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON pa.currency = exc_last.currency
WHERE CASE WHEN pa.cost_type = 'commission' AND pa.date >= '2023-09-25' THEN FALSE ELSE TRUE END
GROUP BY 1,6,8,13

UNION ALL

SELECT
    submitted_date::DATE AS date
     ,'Partnerships'::TEXT AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,store_country AS country
     ,NULL AS customer_type
     ,LOWER(marketing_campaign) AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,NULL::bigint AS conversions
FROM marketing.partnership_validated_orders af
WHERE submitted_date::DATE >= '2023-09-25'
    AND commission_approval = 'APPROVED'
GROUP BY 1,6,8

UNION ALL

SELECT
    ret.date::date AS date
     ,'Retail'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,ret.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,ret.is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(ret.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(ret.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_retail ret
         LEFT JOIN  trans_dev.daily_exchange_rate exc
                    ON ret.date::date = exc.date_
                        AND ret.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON ret.currency = exc_last.currency
GROUP BY 1,6,13

UNION ALL

SELECT
     completed_date::date AS date
     ,'Retail'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,country_name AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(cost) AS total_spent_local_currency
     ,SUM(cost) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.v_retail_subscription_commisson
GROUP BY 1,6,13

UNION ALL

SELECT DISTINCT
     d.datum AS reporting_date
     ,'TV'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,tv.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,tv.brand_non_brand
     ,tv.cash_percentage
     ,tv.non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(COALESCE(tv.gross_actual_spend, tv.gross_budget_spend)/ (DATEDIFF('DAY', tv.date, LAST_DAY(tv.date))+1))
          OVER (PARTITION BY d.datum, tv.country, tv.brand_non_brand, tv.non_cash_percentage) AS total_spent_local_currency
     ,total_spent_local_currency  * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)
          AS total_spent_eur
     ,NULL::bigint AS conversions
FROM public.dim_dates d
         INNER JOIN marketing.marketing_cost_tv tv
                    ON d.datum BETWEEN tv.date AND LAST_DAY(tv.date)
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON tv.date::date = exc.date_
                       AND tv.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON tv.currency = exc_last.currency

UNION ALL

SELECT DISTINCT
     d.datum AS reporting_date
     ,'Billboard'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,bil.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,bil.brand_non_brand
     ,bil.cash_percentage
     ,bil.non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(COALESCE(nullif(bil.gross_actual_spend::text,'cancelled')::decimal(38,8), bil.gross_budget_spend) / (DATEDIFF('DAY', bil.date, LAST_DAY(bil.date))+1))
          OVER (PARTITION BY d.datum, bil.country, bil.brand_non_brand, bil.non_cash_percentage) AS total_spent_local_currency
     ,total_spent_local_currency  * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)
          AS total_spent_eur
     ,NULL::bigint AS conversions
FROM public.dim_dates d
         INNER JOIN marketing.marketing_cost_billboard bil
                    ON d.datum BETWEEN bil.date AND LAST_DAY(bil.date)
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON bil.date::date = exc.date_
                       AND bil.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON bil.currency = exc_last.currency

UNION ALL

SELECT
    reporting_date AS date
     ,'Grover Cash' AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,gc.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
                   COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) AS total_spent_eur
     ,NULL::bigint AS conversions
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON  gc.reporting_date::DATE = exc.date_
                       AND gc.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON gc.currency = exc_last.currency
GROUP BY 1,6

UNION ALL

SELECT
    reporting_date AS date
     ,'Refer Friend' AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,gc.country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,COALESCE(SUM(gc.referral_cost),0) AS total_spent_local_currency
     ,COALESCE(SUM(gc.referral_cost *
                   COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) AS total_spent_eur
     ,NULL::bigint AS conversions
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON  gc.reporting_date::DATE = exc.date_
                       AND gc.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON gc.currency = exc_last.currency
GROUP BY 1,6

UNION ALL

SELECT
    COALESCE(paid_date,submitted_date)::DATE AS date
     ,'Refer Friend' AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,o.store_country AS country
     ,NULL AS customer_type
     ,NULL AS campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,FALSE AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,COALESCE(SUM(o.voucher_discount),0) AS total_spent_local_currency
     ,COALESCE(SUM(o.voucher_discount *
                   COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) AS total_spent_eur
     ,NULL::bigint AS conversions
FROM  master."order" o
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON COALESCE(o.paid_date, o.submitted_date)::DATE = exc.date_
                       AND CASE WHEN store_country = 'United States' THEN 'USD' ELSE 'EUR' END = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON CASE WHEN store_country = 'United States' THEN 'USD' ELSE 'EUR' END = exc_last.currency
WHERE LEFT(voucher_code,3) = 'RF_'
    AND paid_orders >=1
GROUP BY 1,6

UNION ALL

SELECT
    pa.date::date AS date
     ,'Podcasts'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,pa.country
     ,NULL AS customer_type
     ,campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,pa.is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(pa.total_spent_local_currency) AS total_spent_local_currency
     ,SUM(pa.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_podcasts  pa
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON pa.date::date = exc.date_
                       AND pa.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON pa.currency = exc_last.currency
GROUP BY 1,2,6,8,13

UNION ALL

SELECT
    r.date::date AS date
     ,'Reddit'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,r.country
     ,NULL AS customer_type
     ,r.campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,NULL AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(r.cost) AS total_spent_local_currency
     ,SUM(r.cost * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_reddit r
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON r.date::date = exc.date_
                       AND r.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON r.currency = exc_last.currency
GROUP BY 1,2,6,8

UNION ALL

SELECT
    pa.date::date AS date
     ,'Sponsorships'::text AS channel
     ,NULL AS account
     ,NULL AS advertising_channel_type
     ,NULL AS campaign_group_name
     ,pa.country
     ,NULL AS customer_type
     ,campaign_name
     ,NULL::BIGINT AS campaign_id
     ,NULL AS medium
     ,NULL AS ad_set_name
     ,NULL AS ad_name
     ,NULL AS is_test_and_learn
     ,NULL AS brand_non_brand
     ,NULL AS cash_percentage
     ,NULL AS non_cash_percentage
     ,NULL::bigint AS impressions
     ,NULL::bigint AS clicks
     ,SUM(pa.cost) AS total_spent_local_currency
     ,SUM(pa.cost * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
    AS total_spent_eur
     ,NULL::bigint AS conversions
FROM marketing.marketing_cost_daily_sponsorships  pa
         LEFT JOIN trans_dev.daily_exchange_rate exc
                   ON pa.date::date = exc.date_
                       AND pa.currency = exc.currency
         LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
                   ON pa.currency = exc_last.currency
GROUP BY 1,2,6,8;

END TRANSACTION;


BEGIN TRANSACTION;

DROP TABLE IF EXISTS marketing.marketing_cost_daily_base_data;
CREATE TABLE marketing.marketing_cost_daily_base_data AS
SELECT
    base.*
     ,COALESCE(REPLACE(LOWER(base.campaign_name), LOWER(original_campaign_name), LOWER(modified_campaign_name)),
               base.campaign_name) AS campaign_name_modified
FROM marketing_cost_daily_base_data_tmp base
         LEFT JOIN marketing.marketing_campaign_name_modification mc
                   ON POSITION(LOWER(mc.original_campaign_name) IN LOWER(base.campaign_name)) > 0
;

END TRANSACTION;

BEGIN TRANSACTION;
-- Updating is_test_and_learn for google costs data


UPDATE marketing.marketing_cost_daily_base_data
SET is_test_and_learn = TRUE
FROM marketing.marketing_campaigns_test_and_learn_mapping tl
WHERE
        marketing_cost_daily_base_data.channel = tl.channel AND
    CASE
        WHEN tl.account IS NOT NULL THEN tl.account = marketing_cost_daily_base_data.account
        ELSE TRUE
        END AND
    CASE
        WHEN tl.advertising_channel_type IS NOT NULL
            THEN tl.advertising_channel_type = marketing_cost_daily_base_data.advertising_channel_type
        ELSE TRUE
        END AND
        marketing_cost_daily_base_data.campaign_name = tl.campaign_name AND
    marketing_cost_daily_base_data.date::date BETWEEN tl.valid_from::DATE AND tl.valid_until::DATE
;

END TRANSACTION;

GRANT SELECT ON marketing.marketing_cost_daily_base_data TO tableau;

GRANT SELECT ON ALL TABLES IN SCHEMA marketing TO redash_growth;

GRANT SELECT ON marketing.marketing_cost_daily_base_data TO hams;
