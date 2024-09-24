DROP TABLE IF EXISTS hightouch_sources.new_way_of_buying;
CREATE TABLE hightouch_sources.new_way_of_buying AS 
with pricing_costs as (
  with base as (
    select
      distinct subcategory_name
    from
      ods_production.product
  )
  select
    subcategory_name,
    -- REPAIR (note some alterations from Esteban on values)
     CASE 
        WHEN subcategory_name IN ('Headphones', 'Bluetooth Speakers', 'DJ Equipment', 'Hi-Fi Audio', 'Musical Instruments') THEN 22.37
        WHEN subcategory_name IN ('Home Cinema', 'E-Mobility Accessories', 'Phone Accessories', 'TV', 'Streaming Devices', 'Projectors') THEN 70.00
        WHEN subcategory_name IN ('Digital Cameras', 'Lenses', 'Action Cameras', 'Point-and-shoot', 'Camera Accessories', 'CamCorder', 'Instant Cameras', 'Cinema Cameras') THEN 37.51
        WHEN subcategory_name IN ('Laptops', 'Monitors', 'Gaming Computers', 'Desktop Computers', 'Computer Components', 'Computing Accessories', 'All in One PCs', 'For pro', 'For Fun', 'Gaming Accessories', 'Gaming Consoles', 'Virtual Reality', 'Smartphones', 'Tablets', 'e-readers', 'Apple Watches', 'Smartwatches', 'Activity Trackers', 'Sport & GPS Trackers') THEN 33.56
        WHEN subcategory_name IN ('Scooters', 'Bikes') THEN 30.00
        WHEN subcategory_name IN ('Air Treatment', 'Robot Cleaners', 'Smart Appliances', 'Light & Electronics', 'Home Appliances', 'Intelligent Security', 'Coffee Machines', 'Smart Baby Care') THEN 17.74
        ELSE 25.0
    END AS median_repair_pppct,
    CASE 
        WHEN subcategory_name IN ('Headphones', 'Bluetooth Speakers', 'DJ Equipment', 'Hi-Fi Audio', 'Musical Instruments') THEN 11.00
        WHEN subcategory_name IN ('Home Cinema', 'E-Mobility Accessories', 'Phone Accessories', 'TV', 'Streaming Devices', 'Projectors') THEN 5.00
        WHEN subcategory_name IN ('Digital Cameras', 'Lenses', 'Action Cameras', 'Point-and-shoot', 'Camera Accessories', 'CamCorder', 'Instant Cameras', 'Cinema Cameras') THEN 2.50
        WHEN subcategory_name IN ('Laptops', 'Monitors', 'Gaming Computers', 'Desktop Computers', 'Computer Components', 'Computing Accessories', 'All in One PCs', 'For pro', 'For Fun', 'Gaming Accessories', 'Gaming Consoles', 'Virtual Reality', 'Smartphones', 'Tablets', 'e-readers', 'Apple Watches', 'Smartwatches', 'Activity Trackers', 'Sport & GPS Trackers') THEN 10.00
        WHEN subcategory_name IN ('Scooters', 'Bikes') THEN 20.90
        WHEN subcategory_name IN ('Air Treatment', 'Robot Cleaners', 'Smart Appliances', 'Light & Electronics', 'Home Appliances', 'Intelligent Security', 'Coffee Machines', 'Smart Baby Care') THEN 5.00
        ELSE 25.0
    END AS repair_rate_pct,
    -- REFURB
    CASE
      WHEN subcategory_name IN (
        'Headphones',
        'Bluetooth Speakers',
        'DJ Equipment',
        'Hi-Fi Audio',
        'Musical Instruments',
        'Gaming Accessories',
        'Gaming Consoles',
        'Virtual Reality',
        'Phone Accessories',
        'Smartphones',
        'Tablets',
        'e-readers',
        'Apple Watches',
        'Smartwatches',
        'Activity Trackers',
        'Sport & GPS Trackers'
      ) THEN 10.67
      WHEN subcategory_name IN (
        'Home Cinema',
        'E-Mobility Accessories',
        'Air Treatment',
        'Robot Cleaners',
        'Smart Appliances',
        'Light & Electronics',
        'Home Appliances',
        'Intelligent Security',
        'Kitchen Appliances',
        'Smart Baby Care'
      ) THEN 20.8
      WHEN subcategory_name = 'Laptops' THEN 36
      WHEN subcategory_name IN (
        'Monitors',
        'Digital Cameras',
        'Lenses',
        'Action Cameras',
        'Point-and-shoot',
        'Camera Accessories',
        'CamCorder',
        'Instant Cameras',
        'Cinema Cameras',
        'Printers & Scanners',
        'For pro',
        'For Fun',
        'Scooters',
        'Bikes',
        'Streaming Devices',
        'Projectors'
      ) THEN 18.18
      else 50.0
    END AS avg_refurb_cost
  from
    base
),
--- SKU level
sku_data as (
  with sku_ranking as (
    with asset_data as (
      select
        product_sku,
        subcategory_name,
        avg(purchase_price_commercial) as avg_ppc,
        sum(purchase_price_commercial) as sum_ppc,
        sum(months_since_purchase) as sum_months_old,
        sum(subscription_revenue) as sum_sub_rev,
        datediff(years, max(purchased_date) :: date, current_date) as most_recent_purchase_year,
        count(distinct asset_id) as total_assets,
        count(
          distinct case
            when datediff(month, purchased_date :: date, current_date) < 6 then asset_id
          end
        ) as assets_last_6_months,
        coalesce(
          100.0 * assets_last_6_months :: float / nullif(total_assets :: float, 0),
          0
        ) as purchasing_rate,
        sum(
          case
            when asset_status_new in ('PERFORMING', 'AT RISK') then purchase_price_commercial
          end
        ) as inv_util_numerator,
        sum(
          case
            when asset_status_new in (
              'PERFORMING',
              'IN REPAIR',
              'REFURBISHMENT',
              'IN STOCK',
              'AT RISK'
            ) then purchase_price_commercial
          end
        ) as inv_util_denom,
        coalesce(
          100.0 * inv_util_numerator :: float / nullif(inv_util_denom :: float, 0),
          0
        ) as inv_util
      from
        master.asset
      group by
        1,
        2
    ),
    repair_estimate as (
      select
        product_sku,
        avg(price_brutto) as avg_rep_cost
      from
        dm_recommerce.repair_cost_estimates
      group by
        1
    )
    select
      a.product_sku,
      -- % of new assets bought in last 6 months, scored
      case
        when a.purchasing_rate > 50 then 1
        when a.purchasing_rate > 10 then 2
        else 3
      end as investment_score,
      -- % inventory utilisation, scored
      case
        when a.inv_util > 80 then 1
        when a.inv_util > 60 then 2
        else 3
      end as inv_util_score,
      -- Estimate of repair cost per SKU as % of avg purchase price, scored
      case
        when 100.0 * avg_rep_cost / nullif(avg_ppc, 0) > 100 then 3
        when 100.0 * avg_rep_cost / nullif(avg_ppc, 0) < 25 then 1
        else 2
      end as repair_score,
      -- Most recent investment, scored
      case
        when most_recent_purchase_year <= 1 then 1
        when most_recent_purchase_year <= 2 then 2
        when most_recent_purchase_year <= 3 then 3
        else 4
      end as investment_purchase_score,
      inv_util,
      100.0 * coalesce(
        (sum_sub_rev / nullif(sum_months_old, 0)) / nullif(sum_ppc, 0),
        0
      ) as avg_monthly_ir,
      t.avg_refurb_cost,
      t.median_repair_pppct / 100.0 * t.repair_rate_pct / 100.0 as avg_repair_cost_pppct,
      avg_ppc
    from
      asset_data a
      left join repair_estimate d on a.product_sku = d.product_sku
      left join pricing_costs t on a.subcategory_name = t.subcategory_name 
  ),
  return_cost as (
    select
      product_sku as product_sku_1,
      least(
        nullif(return_ups_de_de_final, '#N/A') :: float,
        nullif(return_ups_nl_de_final, '#N/A') :: float,
        nullif(return_hermes_de_final, '#N/A') :: float,
        nullif(return_dhl_de_final, '#N/A') :: float
      ) :: float as de_return_cost,
      least(
        nullif(return_ups_de_at_final, '#N/A') :: float,
        nullif(return_ups_nl_at_final, '#N/A') :: float,
        nullif(return_hermes_at_final, '#N/A') :: float,
        nullif(return_dhl_at_final, '#N/A') :: float
      ) :: float as at_return_cost,
      least(
        nullif(return_ups_de_nl_final, '#N/A') :: float,
        nullif(return_ups_nl_nl_final, '#N/A') :: float,
        nullif(return_hermes_nl_final, '#N/A') :: float,
        nullif(return_dhl_nl_final, '#N/A') :: float
      ) :: float as nl_return_cost,
      least(
        nullif(return_ups_de_es_final, '#N/A') :: float,
        nullif(return_ups_nl_es_final, '#N/A') :: float,
        nullif(return_hermes_es_final, '#N/A') :: float,
        nullif(return_dhl_es_final, '#N/A') :: float
      ) :: float as es_return_cost
    from
      staging_airbyte_bi.k8s_shipping_costs_sheet1
  )
  select
    *
  from
    sku_ranking r
    left join return_cost c on r.product_sku = c.product_sku_1
),
--- SUBSCRIPTION level
subscription_data as (
  select
    subscription_id,
    product_sku,
    product_name,
    category_name,
    subcategory_name,
    brand,
    order_created_date :: date,
    coalesce(
      subscription_bo_id,
      subscription_sf_id,
      subscription_id
    ) as contract_id,
    months_required_to_own,
    round(months_between(CURRENT_DATE, order_created_date::date),2) as months_rented,
    minimum_term_months,
    -- cash collected
    subscription_revenue_paid,
    -- cash expected
    subscription_revenue_due,
    subscription_value,
    country_name,
    customer_type,
    case
      when DATEDIFF(
        days,
        minimum_cancellation_date :: date,
        CURRENT_DATE
      ) >= -7 then True
      else False
    end as eligible_for_new_po
  from
    master.subscription s
  where
    cancellation_date is null
    and subscription_id is not null
    and product_sku is not null
    AND (
      (
        s.order_created_date >= '2023-07-25 12:00:00'
        AND s.store_id IN (1, 4, 618, 126, 627, 622, 626, 629) -- DE, AT, ES, B2B DE,ES,AT,NL,US
      )
      OR (
        s.order_created_date :: date >= '2023-07-31'
        AND s.store_id NOT IN (5, 621) -- Just not NL, US, to include partners here
      )
    )
),
-- ASSET level
asset_data as (
  select
    a.subscription_id,
    a.asset_id,
    a.purchase_price_commercial,
    a.asset_status_new,
    spv.final_price
  from
    master.asset a
    left join dm_finance.spv_report_historical spv on a.asset_id = spv.asset_id
  where
    spv.reporting_date :: date = (
      SELECT
        MAX(reporting_date)
      FROM
        dm_finance.spv_report_historical
    )
    and a.subscription_id in (
      select
        distinct subscription_id
      from
        subscription_data
    )
),
collated_data as (
  select
    s.*,
    coalesce(a.asset_status_new, 'UNKNOWN') as asset_status_new,
    coalesce(a.purchase_price_commercial, k.avg_ppc, 0) as pp_commercial,
    coalesce(a.final_price, a.purchase_price_commercial, k.avg_ppc, 0) as asset_value,
    k.investment_score,
    k.inv_util_score,
    k.repair_score,
    k.investment_purchase_score,
    k.inv_util,
    k.avg_monthly_ir,
    k.avg_refurb_cost,
    k.avg_repair_cost_pppct,
    k.avg_repair_cost_pppct * pp_commercial as avg_repair_cost,
    case
      when s.country_name = 'Germany' then coalesce(k.de_return_cost,20)
      when s.country_name = 'Austria' then coalesce(k.at_return_cost,20)
      when s.country_name = 'Netherlands' then coalesce(k.nl_return_cost,20)
      when s.country_name = 'Spain' then coalesce(k.es_return_cost,20)
      else coalesce(k.de_return_cost,20)
    end as return_cost,
    CASE
      WHEN s.months_required_to_own <= 0 THEN 1.3 * pp_commercial
      WHEN s.months_required_to_own IS NULL THEN 1.3 * pp_commercial
      ELSE s.months_required_to_own :: float * s.subscription_value
    END AS old_po_target,
    k.inv_util_score + k.repair_score + k.investment_purchase_score as total_score,
    case
      when k.inv_util >= 90 then least(5, total_score)
      else total_score
    end as total_score_adjusted,
    case
      when total_score_adjusted <= 3 then 1.0
      when total_score_adjusted = 4 then 0.9
      when total_score_adjusted = 5 then 0.85
      when total_score_adjusted = 6 then 0.8
      when total_score_adjusted = 7 then 0.7
      when total_score_adjusted >= 8 then 0.5
    end as multiplier_1,
    0.01 * POW(total_score_adjusted -3, 2) as multiplier_2,
    case
      when total_score_adjusted = 3 then 0
      when total_score_adjusted = 4 then 0.5
      else 1.0
    end as multiplier_3,
    asset_value * multiplier_1 as final_asset_value,
    (
      avg_refurb_cost + return_cost +(avg_repair_cost * multiplier_3)
    ) * multiplier_2 as final_cost_of_return,
    subscription_revenue_paid * 0.05 as final_reward_value,
    final_asset_value - final_cost_of_return - final_reward_value as new_po_value,
    greatest(old_po_target - subscription_revenue_paid, 1) as old_po_value,
    greatest(old_po_value, new_po_value) as final_po_value
  from
    subscription_data s
    left join asset_data a on a.subscription_id = s.subscription_id
    left join sku_data k on k.product_sku = s.product_sku
)
SELECT
  contract_id, 
case when eligible_for_new_po = TRUE and asset_status_new = 'PERFORMING' then TRUE
else FALSE
end as eligible, 
final_po_value,
case 
when eligible_for_new_po = FALSE and asset_status_new <> 'PERFORMING' then 'Subscription has not reached minimum duration. Asset is marked as not performing; Check payments.'
when eligible_for_new_po = FALSE and asset_status_new =  'PERFORMING' then 'Subscription has not reached minimum duration.'
when eligible_for_new_po = TRUE and asset_status_new <> 'PERFORMING' then 'Asset is marked as not performing; Check payments.'
when eligible_for_new_po = TRUE and asset_status_new = 'PERFORMING' then 'Eligible for purchase quote.'
end as reason
FROM
  collated_data
;
  

  
BEGIN TRANSACTION;

DELETE FROM hightouch_sources.new_way_of_buying_historical
WHERE new_way_of_buying_historical.date = current_date::date
	OR date_trunc('month',new_way_of_buying_historical.date) <= dateadd('year', -1, current_date)
	OR new_way_of_buying_historical.date <> last_day(new_way_of_buying_historical.date);

INSERT INTO hightouch_sources.new_way_of_buying_historical
SELECT
	*,
	current_date AS date
FROM hightouch_sources.new_way_of_buying
;
 
END TRANSACTION;

GRANT SELECT ON hightouch_sources.new_way_of_buying_historical TO  redash_pricing ;
GRANT SELECT ON hightouch_sources.new_way_of_buying_historical TO  group pricing ;
GRANT SELECT ON hightouch_sources.new_way_of_buying_historical TO  hightouch_pricing ;
GRANT SELECT ON hightouch_sources.new_way_of_buying_historical TO  hightouch;
GRANT SELECT ON hightouch_sources.new_way_of_buying_historical TO tableau;