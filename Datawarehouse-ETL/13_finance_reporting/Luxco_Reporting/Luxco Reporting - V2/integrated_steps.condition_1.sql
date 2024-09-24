--Union source - Condition One

DROP TABLE IF EXISTS ods_spv_historical.union_sources_1;
CREATE TABLE ods_spv_historical.union_sources_1 as
SELECT
   *
FROM
   ods_spv_historical.union_sources_202407_eu_all 
WHERE
   region = 'GERMANY'
 AND src IN ('AMAZON','REBUY','MEDIAMARKT_DIRECT_FEED','SATURN_DIRECT_FEED','MR');


--Step2: Removing Outliers
DROP TABLE IF EXISTS ods_spv_historical.spv_used_asset_price_1;
create table ods_spv_historical.spv_used_asset_price_1 as with d as
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
            and reporting_month::date < '2024-08-01'::date
         union all
         select distinct
            date_trunc('month', week_date::date)::date::date as reporting_month,
            trim(product_sku) as product_sku,
            price::double precision as avg_mm_price
         from
            staging_price_collection.ods_saturn
         where
            week_date::date < '2024-08-01'::date
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
      ods_spv_historical.union_sources_1 s		---*************
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
      date_trunc('month', a.purchased_date)::date as reporting_month,
      trim(a.product_sku) as product_sku,
      avg(COALESCE(c.gross_purchase_price_corrected,a.initial_price)) AS avg_pp_price
   FROM
      ods_production.asset a
      INNER JOIN
         finance.warehouse_details b
         ON a.warehouse = b.warehouse_name
         AND b.warehouse_region = 'Europe' 			---*************
         AND b.is_active IS TRUE
       LEFT JOIN 
         finance.asset_sale_m_and_g_sap c
         ON a.asset_id = c.asset_id   
   WHERE
     -- purchased_date::date <= current_date
     purchased_date::date <= '2024-07-31'
   group by
      1,
      2
)
,
h as
(
   select DISTINCT
      date_trunc('month', s.extract_date)::date as reporting_month,
      s.product_sku,
      mm.avg_pp_price
   from
      ods_spv_historical.union_sources_1 s		---*************
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
      ods_spv_historical.union_sources_1 s		---*************
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
   '2024-08-01'::date - 1 as reporting_date,
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
      and
      (
         fi.item_id is null
         or fi.item_id = prep.item_id
      )
where
   (
(ref_price is null)
      or
      (
(coeff - c.median_coeff) between - 10.00 and 10.00
      )
   )
   or fi.id is not null 	
order by
   3 desc ;
  
  -------------------------------------------------
  ------------------------------------------------
  ------------------------------------------------
 drop table if exists ods_spv_historical.spv_used_asset_price_master_1;
---*************
create table ods_spv_historical.spv_used_asset_price_master_1 as ---*************
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
      ods_spv_historical.spv_used_asset_price_1 u		---*************
   WHERE
      u.asset_condition <> 'Others'::text
      AND u.product_sku IS NOT NULL
      AND u.price IS NOT NULL
      and u.reporting_date = '2024-07-31'::date 		---*************
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
   '2024-08-01'::date - 1 as reporting_date,
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
) AS max_available_price_date, datediff(MONTH, b.max_available_reporting_month::date, '2024-07-31'::Date) AS months_since_last_price, power(0.97, datediff(MONTH, b.max_available_reporting_month::date, '2024-07-31'::Date)) * avg(
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
   
  ------------------------------------------------
  ------------------------------------------------
  ------------------------------------------------
  DROP TABLE IF EXISTS ods_spv_historical.price_per_condition_1;
---*************
create table ods_spv_historical.price_per_condition_1 AS---*************
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
      ods_spv_historical.spv_used_asset_price_master_1 b		---*************
   where
      b.reporting_date = '2024-07-31'::date
   GROUP BY
      b.product_sku
)
SELECT
   '2024-07-31'::date as reporting_date,
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