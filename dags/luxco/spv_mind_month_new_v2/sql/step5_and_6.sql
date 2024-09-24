drop table if exists ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}_eu;
---*************
create table ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}_eu as---*************
with d AS
(
   SELECT
      asset.asset_id,
      asset.product_sku,
      asset.purchased_date,
      max(asset.purchased_date) OVER (PARTITION BY asset.product_sku) AS last_sku_purchase,
      COALESCE(c.gross_purchase_price_corrected,asset.initial_price) AS initial_price
   FROM
      ods_production.asset asset 
      inner join
         finance.warehouse_details b
         on asset.warehouse = b.warehouse_name
      LEFT JOIN   
         finance.asset_sale_m_and_g_sap c 
         ON asset.asset_id = c.asset_id
   WHERE
      asset.initial_price IS NOT NULL
      AND asset.initial_price > 0
      and b.warehouse_region = 'Europe'
      and b.is_active is TRUE    ---*************
)
,
e AS
(
   SELECT DISTINCT
      d_1.product_sku,
      avg(d_1.initial_price) AS initial_price
   FROM
      d d_1
   WHERE
      d_1.purchased_date = d_1.last_sku_purchase
   GROUP BY
      d_1.product_sku
)
,
f AS
(
   SELECT DISTINCT
      ss.asset_id,
      CASE
         /*###############
WRITTEN OFF DC
####################*/
         WHEN
            ss.asset_status_original like 'WRITTEN OFF%'
         then
            0           /*###############
LOST/ LOST SOLVED / SOLD
#####################*/
         WHEN
            ss.asset_status_original in
            (
               'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            0           /*###############
USED AND IN STOCK FOR 6M
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND ss.last_allocation_days_in_stock >=
            (
               6 * 30
            )
            AND
            (
               c.m_since_used_price_standardized is null
               or COALESCE(c.m_since_used_price_standardized, 0) > 6
            )
         THEN
            0           /*###############
USED and USED price available
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND c.used_price_standardized IS NOT NULL
         THEN
            c.used_price_standardized           /*###############
NEW but IN STOCK FOR 6m
#####################*/
            -- WHEN ss.asset_condition_spv = 'NEW'
            -- AND COALESCE(ss.delivered_allocations, 0) = 0
            -- AND ss.last_allocation_days_in_stock >= (6 * 30)
            -- AND (c.m_since_neu_price is null or COALESCE(c.m_since_neu_price,0)) > 6
            -- THEN 0
            /*###############
NEW was never delivered -
new price if available
else greatest from agan price and depreciated purchase/price & rrp
#####################*/
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
            AND COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)))             /*###############
IN STOCK FOR 6m
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
            )
            -- OR ss.asset_condition_spv = 'NEW')
            -- AND COALESCE(ss.delivered_allocations, 0) > 0
            AND ss.last_allocation_days_in_stock >=
            (
               6 * 30
            )
            AND
            (
               c.m_since_agan_price_standardized is null
               or COALESCE(c.m_since_agan_price_standardized, 0) > 6
            )
         THEN
            0           /*###############
AGAN, greatest of used and initial price depreciated
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
               OR ss.asset_condition_spv = 'NEW'
               AND COALESCE(ss.delivered_allocations, 0) > 0
            )
            AND COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized, COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)))
         WHEN
            (
(1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)
            )
            <= 0
         THEN
            0
         ELSE
(1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)
      END
      AS residual_value_market_price,
            CASE
         /*###############
LOST/ LOST SOLVED / SOLD
#####################*/
         WHEN
            ss.asset_status_original in
            (
               'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            0           /*###############
USED AND IN STOCK FOR 6M
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND ss.last_allocation_days_in_stock >=
            (
               6 * 30
            )
            AND
            (
               c.m_since_used_price_standardized is null
               or COALESCE(c.m_since_used_price_standardized, 0) > 6
            )
         THEN
            0           /*###############
USED and USED price available
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND c.used_price_standardized IS NOT NULL
         THEN
            c.used_price_standardized           /*###############
NEW but IN STOCK FOR 6m
#####################*/
            -- WHEN ss.asset_condition_spv = 'NEW'
            -- AND COALESCE(ss.delivered_allocations, 0) = 0
            -- AND ss.last_allocation_days_in_stock >= (6 * 30)
            -- AND (c.m_since_neu_price is null or COALESCE(c.m_since_neu_price,0)) > 6
            -- THEN 0
            /*###############
NEW was never delivered -
new price if available
else greatest from agan price and depreciated purchase/price & rrp
#####################*/
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
            AND COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)))             /*###############
IN STOCK FOR 6m
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
            )
            -- OR ss.asset_condition_spv = 'NEW')
            -- AND COALESCE(ss.delivered_allocations, 0) > 0
            AND ss.last_allocation_days_in_stock >=
            (
               6 * 30
            )
            AND
            (
               c.m_since_agan_price_standardized is null
               or COALESCE(c.m_since_agan_price_standardized, 0) > 6
            )
         THEN
            0           /*###############
AGAN, greatest of used and initial price depreciated
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
               OR ss.asset_condition_spv = 'NEW'
               AND COALESCE(ss.delivered_allocations, 0) > 0
            )
            AND COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized, COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)))
         WHEN
            (
(1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)
            )
            <= 0
         THEN
            0
         ELSE
(1 - 0.03 * ((date_trunc('month', '{{ params.first_day_of_month }}'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price)
      END
      AS residual_value_market_price_without_written_off,
      CASE
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND c.m_since_used_price_standardized <= 1
         THEN
            c.used_price_standardized
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
            AND c.m_since_neu_price <= 1
         THEN
            c.new_price_standardized
         WHEN
            (
(ss.asset_condition_spv = 'AGAN'
               OR ss.asset_condition_spv = 'NEW')
               AND COALESCE(ss.delivered_allocations, 0) > 0
            )
            AND c.m_since_agan_price_standardized <= 1
         THEN
            c.agan_price_standardized
         ELSE
            NULL
      END
      AS average_of_sources_on_condition_this_month,
      CASE
         WHEN
            ss.asset_condition_spv = 'AGAN'
            OR ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) > 0
         THEN
            c.agan_price_standardized_before_discount
         WHEN
            ss.asset_condition_spv = 'USED'
         THEN
            c.used_price_standardized_before_discount
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
         THEN
            c.neu_price_before_discount
         ELSE
            NULL
      END
      AS average_of_sources_on_condition_last_available_price,
      CASE
         WHEN
            ss.asset_condition_spv = 'AGAN'
            OR ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) > 0
         THEN
            c.m_since_agan_price_standardized
         WHEN
            ss.asset_condition_spv = 'USED'
         THEN
            c.m_since_used_price_standardized
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
         THEN
            c.m_since_neu_price
         ELSE
            NULL
      END
      AS m_since_last_valuation_price,
      CASE
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
         THEN
            c.new_price_standardized
         WHEN
            ss.asset_condition_spv = 'AGAN'
            OR ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) > 0
         THEN
            c.agan_price_standardized
         WHEN
            ss.asset_condition_spv = 'USED'
         THEN
            c.used_price_standardized
         ELSE
            NULL
      END
      AS valuation_1,
      CASE
         WHEN
            ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) = 0
         THEN
            c.agan_price_standardized
         WHEN
            ss.asset_condition_spv = 'AGAN'
            OR ss.asset_condition_spv = 'NEW'
            AND COALESCE(ss.delivered_allocations, 0) > 0
         THEN
            c.used_price_standardized
         ELSE
            NULL
      END
      AS valuation_2,
      (
         1 - 0.03 * ((date_trunc('month', '{{ params.last_day_of_prev_month }}'::date)::date - ss.purchased_date) / 30)
      )
      * COALESCE(f.gross_purchase_price_corrected,ss.initial_price, ss.amount_rrp, e.initial_price) AS depreciated_purchase_price
   FROM
      master.asset_historical ss
      inner join
         finance.warehouse_details b
         on ss.warehouse = b.warehouse_name
      LEFT JOIN
         (
            select
               *
            from
               ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}_eu
            where
               reporting_date::date =
               (
                  '{{ params.last_day_of_prev_month }}'::date::date
               )
         )
         c
         ON c.product_sku = ss.product_sku
      LEFT JOIN
         e
         ON e.product_sku = ss.product_sku
      LEFT JOIN   
         finance.asset_sale_m_and_g_sap f 
         ON ss.asset_id = f.asset_id
   where
      ss."date"::date =
      (
         '{{ params.mid_day_of_month }}'::date
      )
      and b.warehouse_region = 'Europe'
      and b.is_active is TRUE       ---*************
)
SELECT distinct
   '{{ params.last_day_of_prev_month }}'::Date as reporting_date,
   f.asset_id,
   CASE
      WHEN
         f.residual_value_market_price < 0::double precision
      THEN
         0::double precision
      ELSE
         f.residual_value_market_price
   END
   AS residual_value_market_price,
   CASE
      WHEN
         f.residual_value_market_price_without_written_off < 0::double precision
      THEN
         0::double precision
      ELSE
         f.residual_value_market_price_without_written_off
   END
   AS residual_value_market_price_without_written_off,
   NULL::text AS residual_value_market_price_label, f.average_of_sources_on_condition_this_month, f.m_since_last_valuation_price,
   CASE
      WHEN
         f.m_since_last_valuation_price is null
         or f.m_since_last_valuation_price > 6
      THEN
         '12.1- (c) - (iii)'::text
      ELSE
         '12.1- (c) - (i),(ii)'::text
   END
   AS valuation_method, f.valuation_1,
   CASE
      WHEN
         f.valuation_1 IS NULL
      THEN
         f.valuation_2
      ELSE
         NULL::numeric
   END
   AS valuation_2,   -- f.depreciated_purchase_price AS valuation_3,
   case
      when
         f.depreciated_purchase_price < 0::double precision
      THEN
         0::double precision
      else
         f.depreciated_purchase_price
   end
   AS valuation_3, f.average_of_sources_on_condition_last_available_price
FROM
   f;
------STEP 6--------
drop table if exists ods_production.spv_report_master_{{ params.tbl_suffix }}_eu;
---*************
create table ods_production.spv_report_master_{{ params.tbl_suffix }}_eu as---*************
SELECT distinct
   '{{ params.last_day_of_prev_month }}'::date as reporting_date,
   a.asset_id,
   a.warehouse,
   a.serial_number,
   a.product_sku,
   a.asset_name,
   p.category_name as category,
   p.subcategory_name as subcategory,
   a.country,
   a.city,
   a.postal_code,
   a.invoice_number,
   a.invoice_date,
   a.invoice_url,
   a.purchased_date,
   COALESCE(c.gross_purchase_price_corrected,a.initial_price) AS initial_price,
   a.delivered_allocations,
   a.returned_allocations,
   a.last_allocation_days_in_stock,
   a.asset_condition_spv,
   COALESCE (use.currency, a.currency ) AS currency,
   COALESCE(a.sold_date::date, '{{ params.mid_day_of_month }}'::date) - a.purchased_date::date AS days_since_purchase,
   mv.valuation_method,
   mv.average_of_sources_on_condition_this_month,
   mv.average_of_sources_on_condition_last_available_price,
   mv.m_since_last_valuation_price,
   mv.valuation_1,
   mv.valuation_2,
   mv.valuation_3,
   mv.residual_value_market_price as final_price,
   mv.residual_value_market_price_without_written_off as final_price_without_written_off,
   a.sold_date AS sold_date,
   a.sold_price,
   a.asset_status_original,
   a.subscription_revenue_due AS mrr,
   a.subscription_revenue as collected_mrr,
   null::double precision as total_inflow,
   a.capital_source_name,
   null as exception_rule,
   a.ean
FROM
   master.asset_historical a
   INNER JOIN
      finance.warehouse_details b
      ON a.warehouse = b.warehouse_name
      and b.warehouse_region = 'Europe'
      and b.is_active is TRUE    ---*************
   left join
      ods_production.asset a2
      on a.asset_id = a2.asset_id
   LEFT JOIN
      ods_production.product p
      ON p.product_sku = a.product_sku
   LEFT JOIN
      (
         SELECT DISTINCT
            product_sku,
            currency
         FROM
            ods_spv_historical.union_sources_{{ params.tbl_suffix }}_eu
      )
      use
      ON use.product_sku = a.product_sku
   LEFT JOIN
      (
         select
            *
         from
            ods_spv_historical.asset_market_value_{{ params.tbl_suffix }}_eu
         where
            reporting_date =
            (
               '{{ params.last_day_of_prev_month }}'::date
            )
      )
      mv    --tgt_dev.asset_market_value_dec2 mv ---*************
      ON mv.asset_id = a.asset_id
    LEFT JOIN  
        finance.asset_sale_m_and_g_sap c  
       ON a.asset_id = c.asset_id
WHERE
   a.asset_status_original not in
   (
      'DELETED',
      'BUY NOT APPROVED',
      'BUY',
      'RETURNED TO SUPPLIER'
   )
   and a."date" =
   (
      '{{ params.mid_day_of_month }}'::date
   )
