--Step2: Removing Outliers
DROP TABLE IF EXISTS ods_spv_historical.spv_used_asset_price_202305_us;
create table ods_spv_historical.spv_used_asset_price_202305_us as with d as
(
   /*#############################################
Calculating Avg MM and Saturn price per month
###############################################*/
   select
      reporting_month,
      product_sku,
      avg(avg_mm_price) as avg_mm_price
   from
      (
         select distinct
            date_trunc('month', reporting_month::date)::date::date as reporting_month,
            trim(product_sku) as product_sku,
            price::double precision as avg_mm_price
         from
            staging_price_collection.ods_mediamarkt
         where
            asset_condition = 'Neu'
            and src = 'MEDIAMARKT_DIRECT_FEED'
            and reporting_month::date < '2023-06-01'::date
         union all
         select distinct
            date_trunc('month', week_date::date)::date::date as reporting_month,
            trim(product_sku) as product_sku,
            price::double precision as avg_mm_price
         from
            staging_price_collection.ods_saturn
         where
            week_date::date < '2023-06-01'::date
      )
   group by
      1,
      2
)
,
b as
(
   /*##################################################
Joining Avg MM price per month with previous step
###################################################*/
   select DISTINCT
      date_trunc('month', s.extract_date)::date as reporting_month,
      s.product_sku,
      mm.avg_mm_price
   from
      ods_spv_historical.union_sources_202305_us s		---*************
      left join
         d mm
         on date_trunc('month', s.extract_date::date) = mm.reporting_month
         and s.product_sku = mm.product_sku
   order by
      1
)
,
e as
(
   /*################################
Filling MM prices where its empty
#################################*/
   select DISTINCT
      reporting_month,
      product_sku,
      avg_mm_price,
      lead(avg_mm_price) ignore nulls over (partition by product_sku
   order by
      reporting_month) as next_price,
      lag(avg_mm_price) ignore nulls over (partition by product_sku
   order by
      reporting_month) as previous_price
   FROM
      b
   order by
      reporting_month
)
,
mm as
(
   select
      reporting_month,
      product_sku,
      COALESCE(avg_mm_price, next_price, previous_price) as avg_mm_price
   from
      e
)
,
g as
(
   SELECT distinct
      date_trunc('month', purchased_date)::date as reporting_month,
      trim(asset.product_sku) as product_sku,
      avg(asset.initial_price) AS avg_pp_price
   FROM
      ods_production.asset
      inner join
         finance.warehouse_details b
         on asset.warehouse = b.warehouse_name
   where
      --purchased_date::date <= '2022-12-31'::date
      purchased_date::date <= '2023-05-31'
      and b.warehouse_region = 'North America'
      and b.is_active is TRUE		---*************
   group by 1, 2
)
,
h as
(
   select DISTINCT
      date_trunc('month', s.extract_date)::date as reporting_month,
      s.product_sku,
      mm.avg_pp_price
   from
      ods_spv_historical.union_sources_202305_us s		---*************
      left join
         g mm
         on date_trunc('month', s.extract_date::date) = mm.reporting_month
         and s.product_sku = mm.product_sku
   order by
      1
)
,
j as
(
   select DISTINCT
      reporting_month,
      product_sku,
      avg_pp_price,
      lead(avg_pp_price) ignore nulls over (partition by product_sku
   order by
      reporting_month) as next_price,
      lag(avg_pp_price) ignore nulls over (partition by product_sku
   order by
      reporting_month) as previous_price
   FROM
      h
   order by
      reporting_month
)
,
check_ as
(
   select
      reporting_month,
      product_sku,
      COALESCE(avg_pp_price, next_price, previous_price) as avg_pp_price
   from
      j
)
,
prep as
(
   select
      s.*,
      mm.avg_mm_price,
      c.avg_pp_price,
      coalesce(mm.avg_mm_price, c.avg_pp_price*1.1) as ref_price,
      round((s.price::double precision /
      case
         when
            coalesce(mm.avg_mm_price, c.avg_pp_price*1.1) = 0
         then
            null
         else
            coalesce(mm.avg_mm_price, c.avg_pp_price*1.1)
      end
)* 100, 2) as coeff,
      case
         when
            round((s.price::double precision /
            case
               when
                  coalesce(mm.avg_mm_price, c.avg_pp_price*1.1) = 0
               then
                  null
               else
                  coalesce(mm.avg_mm_price, c.avg_pp_price*1.1)
            end
)* 100, 2) is null
         then
            null
         else
            MEDIAN(round((s.price::double precision /
            case
               when
                  coalesce(mm.avg_mm_price, c.avg_pp_price*1.1) = 0
               then
                  null
               else
                  coalesce(mm.avg_mm_price, c.avg_pp_price*1.1)
            end
)* 100, 2)) OVER (partition by s.product_sku, asset_condition)
      end
      as median_coeff
   from
      ods_spv_historical.union_sources_202305_us s		---*************
      inner join
         ods_production.product p
         on p.product_sku = s.product_sku
      left join
         mm
         on date_trunc('month', s.extract_date::date) = mm.reporting_month
         and s.product_sku = mm.product_sku
      left join
         check_ c
         on date_trunc('month', s.extract_date::date) = c.reporting_month
         and s.product_sku = c.product_sku
   where
      true
)
, c as
(
   select
      product_sku,
      asset_condition,
      avg(median_coeff) as median_coeff
   from
      prep
   group by
      1,
      2
)
select
   '2023-06-01'::date - 1 as reporting_date,
   prep.*,
   max(prep.extract_date::date) OVER (PARTITION BY prep.src, prep.product_sku, prep.asset_condition) AS max_available_date,
   rank() OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.extract_date
ORDER BY
   price, prep.item_id, prep.extract_date) AS price_rank,
   rank() OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.reporting_month
ORDER BY
   price, prep.item_id, prep.extract_date) AS price_rank_month
from
   prep
   left join
      c
      on c.product_sku = prep.product_sku
      and c.asset_condition = prep.asset_condition
   left join
      ods_spv_historical.force_inclusions fi
      on fi.product_sku = prep.product_sku
      and fi.asset_condition = prep.asset_condition
      and fi.extract_date = prep.extract_date
      and fi.item_id is null
      or fi.item_id = prep.item_id
where
   (
(ref_price is null)
      or
      (
(coeff - c.median_coeff) between - 10.00 and 10.00
      )
   )
   or fi.id is not null 	/*
or (prep.product_sku = 'GRB18P1756' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-05-03')
or (prep.product_sku = 'GRB94P2979' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-05-27')
--or (prep.product_sku = 'GRB74P3978' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-01-01')
or (prep.product_sku = 'GRB224P10020' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-06-21')
--((coeff::int >= median_coeff::int-15) or (coeff::int <= median_coeff::int+15)))
----FORCE inclusion in Novemeber 2021
OR(prep.product_sku = 'GRB18P4315' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB18P4313' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB18P4014' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB224P10914' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-30')
OR(prep.product_sku = 'GRB59P10386' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22' AND price='576.99')
OR(prep.product_sku = 'GRB224P10035' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB94P1702' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-29')
OR(prep.product_sku = 'GRB18P2644' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB59P10520' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB224P10021' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')
OR(prep.product_sku = 'GRB224P10642' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-30')
OR(prep.product_sku = 'GRB18P2634' and prep.asset_condition = 'Wie neu' and prep.extract_date = '2021-11-22')*/
order by
   3 desc ;
------STEP 3---------
drop table if exists ods_spv_historical.spv_used_asset_price_master_202305_us;
---*************
create table ods_spv_historical.spv_used_asset_price_master_202305_us as ---*************
WITH a AS
(
   SELECT
      u.extract_date::text as reporting_month,
      u.product_sku,
      u.asset_condition,
      u.src,
      u.max_available_date as max_available_reporting_month,
      avg(
      CASE
         WHEN
            u.price_rank <= 3
         THEN
            u.price
         ELSE
            NULL
      END
) AS avg_3_lowest_price
   FROM
      ods_spv_historical.spv_used_asset_price_202305_us u		---*************
   WHERE
      u.asset_condition <> 'Others'::text
      AND u.product_sku IS NOT NULL
      AND u.price IS NOT NULL
      and u.reporting_date = '2023-05-31'::date 		---*************
   GROUP BY
      u.extract_date, u.product_sku, u.asset_condition, u.src, u.max_available_date
   ORDER BY
      u.product_sku DESC
)
, b as
(
   select
      product_sku,
      asset_condition,
      max(max_available_reporting_month) AS max_available_reporting_month
   FROM
      a
   group by
      1,
      2
)
SELECT
   '2023-06-01'::date - 1 as reporting_date,
   a.product_sku,
   a.asset_condition,
   avg(
   CASE
      WHEN
         date_trunc('month', a.reporting_month::date) = date_trunc('month', b.max_available_reporting_month::date)
      THEN
         a.avg_3_lowest_price
      ELSE
         NULL::numeric
   END
) AS avg_price, max(
   CASE
      WHEN
         date_trunc('month', a.reporting_month::date) = date_trunc('month', b.max_available_reporting_month::date)
      THEN
         a.reporting_month
      ELSE
         NULL::text
   END
) AS max_available_price_date, datediff(MONTH, b.max_available_reporting_month::date, '2023-05-31'::Date) AS months_since_last_price, power(0.97, datediff(MONTH, b.max_available_reporting_month::date, '2023-05-31'::Date)) * avg(
   CASE
      WHEN
         date_trunc('month', a.reporting_month::date) = date_trunc('month', b.max_available_reporting_month::date)
      THEN
         a.avg_3_lowest_price
      ELSE
         NULL::numeric
   END
) AS final_market_price
FROM
   a
   left join
      b
      on a.product_sku = b.product_sku
      and a.asset_condition = b.asset_condition
GROUP BY
   a.product_sku, a.asset_condition, b.max_available_reporting_month;
------STEP 4-----------------
DROP TABLE IF EXISTS ods_spv_historical.price_per_condition_202305_us;
---*************
create table ods_spv_historical.price_per_condition_202305_us AS---*************
WITH a AS
(
   SELECT DISTINCT
      b.product_sku,
      max(
      CASE
         WHEN
            b.asset_condition = 'Neu'::text
         THEN
            b.final_market_price
         ELSE
            NULL
      END
) AS neu_price, max(
      CASE
         WHEN
            b.asset_condition = 'Wie neu'::text
         THEN
            b.final_market_price
         ELSE
            NULL
      END
) AS as_good_as_new_price, max(
      CASE
         WHEN
            b.asset_condition = 'Sehr gut'::text
         THEN
            b.final_market_price
         ELSE
            NULL
      END
) AS sehr_gut_price, max(
      CASE
         WHEN
            b.asset_condition = 'Gut'::text
         THEN
            b.final_market_price
         ELSE
            NULL
      END
) AS gut_price, max(
      CASE
         WHEN
            b.asset_condition = 'Akzeptabel'::text
         THEN
            b.final_market_price
         ELSE
            NULL
      END
) AS akzeptabel_price, max(
      CASE
         WHEN
            b.asset_condition = 'Neu'::text
         THEN
            b.avg_price
         ELSE
            NULL
      END
) AS neu_price_before_discount, max(
      CASE
         WHEN
            b.asset_condition = 'Wie neu'::text
         THEN
            b.avg_price
         ELSE
            NULL
      END
) AS as_good_as_new_price_before_discount, max(
      CASE
         WHEN
            b.asset_condition = 'Sehr gut'::text
         THEN
            b.avg_price
         ELSE
            NULL
      END
) AS sehr_gut_price_before_discount, max(
      CASE
         WHEN
            b.asset_condition = 'Gut'::text
         THEN
            b.avg_price
         ELSE
            NULL
      END
) AS gut_price_before_discount, max(
      CASE
         WHEN
            b.asset_condition = 'Akzeptabel'::text
         THEN
            b.avg_price
         ELSE
            NULL
      END
) AS akzeptabel_price_before_discount, max(
      CASE
         WHEN
            b.asset_condition = 'Neu'::text
         THEN
            b.months_since_last_price
         ELSE
            NULL
      END
) AS m_since_neu_price, max(
      CASE
         WHEN
            b.asset_condition = 'Wie neu'::text
         THEN
            b.months_since_last_price
         ELSE
            NULL
      END
) AS m_since_as_good_as_new_price, max(
      CASE
         WHEN
            b.asset_condition = 'Sehr gut'::text
         THEN
            b.months_since_last_price
         ELSE
            NULL
      END
) AS m_since_sehr_gut_price, max(
      CASE
         WHEN
            b.asset_condition = 'Gut'::text
         THEN
            b.months_since_last_price
         ELSE
            NULL
      END
) AS m_since_gut_price, max(
      CASE
         WHEN
            b.asset_condition = 'Akzeptabel'::text
         THEN
            b.months_since_last_price
         ELSE
            NULL
      END
) AS m_since_akzeptabel_price
   FROM
      ods_spv_historical.spv_used_asset_price_master_202305_us b		---*************
   where
      b.reporting_date = '2023-05-31'::date
   GROUP BY
      b.product_sku
)
SELECT
   '2023-05-31'::date as reporting_date,
   a.product_sku,
   a.neu_price,
   a.as_good_as_new_price,
   a.sehr_gut_price,
   a.gut_price,
   a.akzeptabel_price,
   a.neu_price_before_discount,
   a.as_good_as_new_price_before_discount,
   a.sehr_gut_price_before_discount,
   a.gut_price_before_discount,
   a.akzeptabel_price_before_discount,
   a.m_since_neu_price,
   a.m_since_as_good_as_new_price,
   a.m_since_sehr_gut_price,
   a.m_since_gut_price,
   a.m_since_akzeptabel_price,
   a.neu_price AS new_price_standardized,
   COALESCE(a.as_good_as_new_price, a.sehr_gut_price) AS agan_price_standardized,
   COALESCE(a.as_good_as_new_price_before_discount, a.sehr_gut_price_before_discount) AS agan_price_standardized_before_discount,
   COALESCE(a.m_since_as_good_as_new_price, a.m_since_sehr_gut_price) AS m_since_agan_price_standardized,
   CASE
      WHEN
         (
            COALESCE(a.sehr_gut_price, 0) + COALESCE(a.gut_price, 0) + COALESCE(a.akzeptabel_price, 0)
         )
         = 0
      THEN
         NULL
      ELSE
(COALESCE(a.sehr_gut_price, 0) + COALESCE(a.gut_price, 0) + COALESCE(a.akzeptabel_price, 0)) / (COALESCE(a.sehr_gut_price - a.sehr_gut_price + 1, 0) + COALESCE(a.gut_price - a.gut_price + 1, 0) + COALESCE(a.akzeptabel_price - a.akzeptabel_price + 1, 0))
   END
   AS used_price_standardized,
   CASE
      WHEN
         (
            COALESCE(a.sehr_gut_price_before_discount, 0) + COALESCE(a.gut_price_before_discount, 0) + COALESCE(a.akzeptabel_price_before_discount, 0)
         )
         = 0
      THEN
         NULL
      ELSE
(COALESCE(a.sehr_gut_price_before_discount, 0) + COALESCE(a.gut_price_before_discount, 0) + COALESCE(a.akzeptabel_price_before_discount, 0)) / (COALESCE(a.sehr_gut_price_before_discount - a.sehr_gut_price_before_discount + 1, 0) + COALESCE(a.gut_price_before_discount - a.gut_price_before_discount + 1, 0) + COALESCE(a.akzeptabel_price_before_discount - a.akzeptabel_price_before_discount + 1, 0))
   END
   AS used_price_standardized_before_discount, LEAST(a.m_since_sehr_gut_price, a.m_since_gut_price, a.m_since_akzeptabel_price) AS m_since_used_price_standardized
FROM
   a;
-----STEP 5--------
drop table if exists ods_spv_historical.asset_market_value_202305_us;
---*************
create table ods_spv_historical.asset_market_value_202305_us as---*************
with d AS
(
   SELECT
      asset.asset_id,
      asset.product_sku,
      asset.purchased_date,
      max(asset.purchased_date) OVER (PARTITION BY asset.product_sku) AS last_sku_purchase,
      asset.initial_price
   FROM
      ods_production.asset
      inner join
         finance.warehouse_details b
         on asset.warehouse = b.warehouse_name
   WHERE
      asset.initial_price IS NOT NULL
      AND asset.initial_price > 0
      and b.warehouse_region = 'North America'
      and b.is_active is TRUE		---*************
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
         /*############### WRITTEN OFF DC ####################*/
         WHEN ss.asset_status_original like 'WRITTEN OFF%' then 0
         /*############### LOST/ LOST SOLVED / SOLD #####################*/
         WHEN ss.asset_status_original in ( 'SOLD', 'LOST', 'LOST SOLVED' )
         THEN 0
         /*############### USED AND IN STOCK FOR 6M #####################*/
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
            0 				/*###############
USED and USED price available
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND c.used_price_standardized IS NOT NULL
         THEN
            c.used_price_standardized 				/*###############
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
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) 				/*###############
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
            0 				/*###############
AGAN, greatest of used and initial price depreciated
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
               OR ss.asset_condition_spv = 'NEW'
               AND COALESCE(ss.delivered_allocations, 0) > 0
            )
            AND COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized, COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
         WHEN
            (
(1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
            )
            <= 0
         THEN
            0
         ELSE
(1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
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
            0 				/*###############
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
            0 				/*###############
USED and USED price available
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'USED'
            )
            AND c.used_price_standardized IS NOT NULL
         THEN
            c.used_price_standardized 				/*###############
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
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) 				/*###############
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
            0 				/*###############
AGAN, greatest of used and initial price depreciated
#####################*/
         WHEN
            (
               ss.asset_condition_spv = 'AGAN'
               OR ss.asset_condition_spv = 'NEW'
               AND COALESCE(ss.delivered_allocations, 0) > 0
            )
            AND COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized, COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
         THEN
            COALESCE(c.agan_price_standardized, GREATEST(c.used_price_standardized,
            (
               1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date::date) / 30)
            )
            * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
         WHEN
            (
(1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
            )
            <= 0
         THEN
            0
         ELSE
(1 - 0.03 * ((date_trunc('month', '2023-05-01'::date - 1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
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
         1 - 0.03 * ((date_trunc('month', '2023-05-31'::date)::date - ss.purchased_date) / 30)
      )
      * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price) AS depreciated_purchase_price
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
               ods_spv_historical.price_per_condition_202305_us
            where
               reporting_date::date =
               (
                  '2023-05-31'::date::date
               )
         )
         c
         ON c.product_sku = ss.product_sku
      LEFT JOIN
         e
         ON e.product_sku = ss.product_sku
   where
      ss."date"::date =
      (
         '2023-05-31'::date
      )
      and b.warehouse_region = 'North America'
      and b.is_active is TRUE 		---*************
)
SELECT distinct
   '2023-05-31'::Date as reporting_date,
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
   NULL::text AS residual_value_market_price_label,
   f.average_of_sources_on_condition_this_month,
   f.m_since_last_valuation_price,
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
   AS valuation_2, 	-- f.depreciated_purchase_price AS valuation_3,
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
drop table if exists ods_production.spv_report_master_202305_us;
---*************
create table ods_production.spv_report_master_202305_us as---*************
SELECT distinct
   '2023-05-31'::date as reporting_date,
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
   a.initial_price,
   a.delivered_allocations,
   a.returned_allocations,
   a.last_allocation_days_in_stock,
   a.asset_condition_spv,
   COALESCE (use.currency, a.currency ) AS currency,
   COALESCE(a.sold_date::date, '2023-06-01'::date) - a.purchased_date::date AS days_since_purchase,
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
   inner join
      finance.warehouse_details b
      on a.warehouse = b.warehouse_name
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
            ods_spv_historical.union_sources_202305_us
      )
      use
      ON use.product_sku = a.product_sku
   LEFT JOIN
      (
         select
            *
         from
            ods_spv_historical.asset_market_value_202305_us
         where
            reporting_date =
            (
               '2023-05-31'::date
            )
      )
      mv		--tgt_dev.asset_market_value_dec2 mv ---*************
      ON mv.asset_id = a.asset_id
WHERE
   a.asset_status_original not in
   (
      'DELETED'::character varying::text,
      'BUY NOT APPROVED'::character varying::text,
      'BUY'::character varying::text,
      'RETURNED TO SUPPLIER'::character varying::text
   )
   and b.warehouse_region = 'North America'
   and b.is_active is TRUE	---*************
   and a."date" =
   (
      '2023-05-31'::date
   )
;

GRANT SELECT ON ods_spv_historical.price_per_condition_202305_us TO tableau;
GRANT SELECT ON ods_production.spv_report_master_202305_us TO tableau;
GRANT SELECT ON ods_spv_historical.price_per_condition_202306_us TO tableau;
GRANT SELECT ON ods_production.spv_report_master_202306_us TO tableau;
