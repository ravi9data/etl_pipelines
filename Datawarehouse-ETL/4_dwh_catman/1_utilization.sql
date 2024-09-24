drop table if exists dwh.utilization;

create table dwh.utilization as 
with supplier as (
select distinct 
 supplier, 
 supplier_account
from ods_production.asset a
),
status as (
SELECT DISTINCT 
         asset_status_original,
         asset_status_grouped
from ods_production.asset a 
where asset_status_grouped in ('IN STOCK','ON RENT','REFURBISHMENT','TRANSITION')
  and asset_status_original not in ('SOLD')
)
,oldd as (
SELECT distinct 
        CASE
            WHEN f.warehouse::text = 'office_us'::text THEN 'office_us'::text
            WHEN f.warehouse::text = 'ups_softeon_us_kylse'::text THEN 'ups_softeon_us_kylse'::text
            WHEN f.warehouse::text in ('synerlogis_de','office_de') THEN 'synerlogis_de'::text
            WHEN f.warehouse::text = 'ups_softeon_eu_nlrng' THEN 'ups_softeon_eu_nlrng'::text
            WHEN f.warehouse::text = 'ingram_micro_eu_flensburg' then 'ingram_micro_eu_flensburg'::text
            ELSE 'others'::text
        END AS warehouse,
        supplier_account,
        f.fact_day::Date as reporting_date,
        NULL::date AS purchased_date,
        f.product_sku,
        p.product_name,
        p.category_name,
        p.subcategory_name,
        p.brand,
                sum(
        f.total_initial_price
        ) as purchase_price,
        sum(
        CASE
            WHEN st.asset_status_grouped= 'IN STOCK'::text THEN f.total_initial_price else 0
        END) as purchase_price_in_stock,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'ON RENT'::text THEN f.total_initial_price  else 0
        END) as purchase_price_on_rent,
        sum(
        CASE
            WHEN st.asset_status_original in ('ON LOAN','OFFICE') THEN f.total_initial_price  else 0
        END) as purchase_price_performing_on_rent,
        sum(
        CASE
            WHEN st.asset_status_original in ('IN DEBT COLLECTION') THEN f.total_initial_price  else 0
        END) as purchase_price_at_risk_on_rent,
		sum(
        CASE
            WHEN st.asset_status_grouped= 'REFURBISHMENT'::text THEN f.total_initial_price  else 0
        END) as purchase_price_refurbishment,
        sum(
        CASE
            WHEN st.asset_status_original = 'IN REPAIR' THEN f.total_initial_price  else 0
        END) as purchase_price_in_repair,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'TRANSITION'::text THEN f.total_initial_price  else 0
        END) as purchase_price_transition,   
        sum(
        CASE
            WHEN st.asset_status_grouped = 'SOLD' THEN f.total_initial_price  else 0
        END) as purchase_price_sold,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'SELLING' THEN f.total_initial_price  else 0
        END) as purchase_price_selling,
      	0::float AS purchase_price_inbound,
      	0::float AS purchase_price_inbound_unallocable,--TO GET same number OF COLUMNs
      	sum(
        CASE
            WHEN st.asset_status_original = 'IRREPARABLE' THEN f.total_initial_price  else 0
        END) as purchase_price_irreparable,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'LOST' THEN f.total_initial_price  else 0
        END) as purchase_price_lost, 
                sum(
        CASE
            WHEN st.asset_status_grouped = 'WRITTEN OFF' THEN f.total_initial_price  else 0
        END) as purchase_price_writtenoff, 
        sum(
        CASE
            WHEN st.asset_status_grouped not in ('SELLING','SOLD','TRANSITION','REFURBISHMENT','PERFORMING','AT RISK','NOT AVAILABLE','IN STOCK', 'IN REPAIR') 
             THEN f.total_initial_price  else 0
        END) as purchase_price_others,      
                        sum(
        f.number_of_assets
        ) as number_of_assets,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'IN STOCK'::text THEN f.number_of_assets else 0
        END) as number_of_assets_in_stock,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'ON RENT'::text THEN f.number_of_assets  else 0
        END) as number_of_assets_on_rent,
		sum(
        CASE
            WHEN st.asset_status_grouped = 'REFURBISHMENT'::text THEN f.number_of_assets  else 0
        END) as number_of_assets_refurbishment,
        sum(
        CASE
            WHEN st.asset_status_original = 'IN REPAIR' THEN f.number_of_assets  else 0
        END) as number_of_assets_in_repair,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'TRANSITION'::text THEN f.number_of_assets  else 0
        END) as number_of_assets_transition,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'SOLD' THEN f.number_of_assets  else 0
        END) as number_of_assets_sold,
        sum(
        CASE
            WHEN st.asset_status_grouped = 'SELLING' THEN f.number_of_assets  else 0
        END) as number_of_assets_selling,
        0::float AS number_of_assets_inbound,
        0::float AS number_of_assets_inbound,--TO GET same number OF COLUMNs 
                sum(
        CASE
            WHEN st.asset_status_grouped = 'LOST' THEN f.number_of_assets  else 0
        END) as number_of_assets_lost,
        		sum(
        CASE
            WHEN st.asset_status_grouped = 'WRITTEN OFF' THEN f.number_of_assets  else 0
        END) as number_of_assets_written_off,
                sum(
        CASE
            WHEN st.asset_status_grouped not in ('SELLING','SOLD','TRANSITION','REFURBISHMENT','PERFORMING','AT RISK','NOT AVAILABLE','IN STOCK', 'IN REPAIR') 
             THEN f.number_of_assets  else 0
        END) as number_of_assets_others
from dwh.asset_sku_supplier_status_daily_fact f 
left join supplier s on s.supplier=f.supplier
inner join status st on st.asset_status_original=f.status
left join ods_production.product p on p.product_sku=f.product_sku
where f.fact_day::Date < '2019-03-01'
group by 1,2,3,4,5,6,7,8,9
order by 3 DESC
)
,neww as (
  SELECT distinct 
        CASE
            WHEN f.warehouse::text = 'office_us'::text THEN 'office_us'::text
            WHEN f.warehouse::text = 'ups_softeon_us_kylse'::text THEN 'ups_softeon_us_kylse'::text
            WHEN f.warehouse::text in ('synerlogis_de','office_de') THEN 'synerlogis_de'::text
            WHEN f.warehouse::text = 'ups_softeon_eu_nlrng' THEN 'ups_softeon_eu_nlrng'::text
            WHEN f.warehouse::text = 'ingram_micro_eu_flensburg' then 'ingram_micro_eu_flensburg'::text
            ELSE 'others'::text
        END AS warehouse,
        supplier as supplier_account,
        f.fact_date::Date as reporting_date,
        f.purchased_date,
        p.product_sku,
        p.product_name,
        p.category_name,
        p.subcategory_name,
        p.brand,
        sum(
        f.total_initial_price
        ) as purchase_price,
        sum(
        CASE
            WHEN f.asset_status_new in ('IN STOCK') THEN f.total_initial_price else 0
        END) as purchase_price_in_stock,
        sum(
        CASE
            WHEN f.asset_status_new in ('PERFORMING','AT RISK','NOT AVAILABLE') THEN f.total_initial_price  else 0
        END) as purchase_price_on_rent,
                sum(
        CASE
            WHEN f.asset_status_new in ('PERFORMING','NOT AVAILABLE') THEN f.total_initial_price  else 0
        END) as purchase_price_performing_on_rent,
                sum(
        CASE
            WHEN f.asset_status_new in ('AT RISK') THEN f.total_initial_price  else 0
        END) as purchase_price_at_risk_on_rent,       
		sum(
        CASE
            WHEN f.asset_status_new = 'REFURBISHMENT' --before Jan 12, in repair and irreparable were in this group
                and f.asset_status_original not in ('IN REPAIR', 'IRREPARABLE') THEN f.total_initial_price  else 0
        END) as purchase_price_refurbishment,
        sum(
        CASE
            WHEN f.asset_status_original = 'IN REPAIR' THEN f.total_initial_price  else 0
        END) as purchase_price_in_repair,
        sum(
        CASE
            WHEN f.asset_status_new = 'TRANSITION' THEN f.total_initial_price  else 0
        END) as purchase_price_transition,      
                sum(
        CASE
            WHEN f.asset_status_new = 'SOLD' THEN f.total_initial_price  else 0
        END) as purchase_price_sold,  
                sum(
        CASE
            WHEN f.asset_status_new = 'SELLING' THEN f.total_initial_price  else 0
        END) as purchase_price_selling, 
        sum(
        CASE 
        	WHEN f.asset_status_new = 'NOT CLASSIFIED' AND f.asset_status_original IN ('INBOUND DAMAGED','INBOUND QUARANTINE') THEN f.total_initial_price ELSE 0 --BI_6044: getting inbound labels info TO comply WITH stakeholders request
        END)::float AS purchase_price_inbound,
        sum(
        CASE 
        	WHEN f.asset_status_new = 'NOT CLASSIFIED' AND f.asset_status_original = 'INBOUND UNALLOCABLE' THEN f.total_initial_price ELSE 0 --BI_6750: separating inbound unallocable AS it will enter AS separate amount the IN stock
        END)::float AS purchase_price_inbound_unallocable,
        sum(
        CASE
            WHEN f.asset_status_original = 'IRREPARABLE' THEN f.total_initial_price  else 0
        END) as purchase_price_irreparable, 
                sum(
        CASE
            WHEN f.asset_status_new = 'LOST' THEN f.total_initial_price  else 0
        END) as purchase_price_lost, 
                sum(
        CASE
            WHEN f.asset_status_new = 'WRITTEN OFF' THEN f.total_initial_price  else 0
        END) as purchase_price_writtenoff, 
                sum(
        CASE
            WHEN f.asset_status_new not in ('SELLING','SOLD','TRANSITION','REFURBISHMENT','PERFORMING','AT RISK','NOT AVAILABLE','IN STOCK', 'IN REPAIR') 
             THEN f.total_initial_price  else 0
        END) as purchase_price_others,  
        sum(
        f.number_of_assets
        ) as number_of_assets,
        sum(
        CASE
            WHEN f.asset_status_new = 'IN STOCK' THEN f.number_of_assets else 0
        END) as number_of_assets_in_stock,
        sum(
        CASE
            WHEN f.asset_status_new in ('PERFORMING','AT RISK','NOT AVAILABLE') THEN f.number_of_assets  else 0
        END) as number_of_assets_on_rent,
		sum(
        CASE
            WHEN f.asset_status_new = 'REFURBISHMENT' --before Jan 12, in repair and irreparable were in this group
                and f.asset_status_original not in ('IN REPAIR', 'IRREPARABLE') THEN f.number_of_assets  else 0
        END) as number_of_assets_refurbishment,
        sum(
        CASE
            WHEN f.asset_status_original = 'IN REPAIR' THEN f.number_of_assets  else 0
        END) as number_of_assets_in_repair,
        sum(
        CASE
            WHEN f.asset_status_new = 'TRANSITION' THEN f.number_of_assets  else 0
        END) as number_of_assets_transition,
                sum(
        CASE
            WHEN f.asset_status_new = 'SOLD' THEN f.number_of_assets  else 0
        END) as number_of_assets_sold,
             sum(
        CASE
            WHEN f.asset_status_new = 'SELLING' THEN f.number_of_assets  else 0
        END) as number_of_assets_selling,
        sum(
        CASE 
        	WHEN f.asset_status_new = 'NOT CLASSIFIED' AND f.asset_status_original IN ('INBOUND DAMAGED','INBOUND QUARANTINE') THEN f.number_of_assets ELSE 0 --BI_6044: getting inbound labels info TO comply WITH stakeholders request
        END)::float AS number_of_assets_inbound,
        sum(
        CASE 
        	WHEN f.asset_status_new = 'NOT CLASSIFIED' AND f.asset_status_original = 'INBOUND UNALLOCABLE' THEN f.number_of_assets ELSE 0 --BI_6750: separating inbound unallocable AS it will enter AS separate amount the IN stock
        END)::float AS number_of_assets_inbound_unallocable,
        sum(
        CASE
            WHEN f.asset_status_new = 'LOST' THEN f.number_of_assets  else 0
        END) as number_of_assets_lost,
        sum(
        CASE
            WHEN f.asset_status_new = 'WRITTEN OFF' THEN f.number_of_assets  else 0
        END) as number_of_assets_written_off,
        sum(
        CASE
            WHEN f.asset_status_new not in ('SELLING','SOLD','TRANSITION','REFURBISHMENT','PERFORMING','AT RISK','NOT AVAILABLE','IN STOCK', 'IN REPAIR') AND (f.asset_status_new != 'NOT CLASSIFIED' OR asset_status_original NOT IN ('INBOUND DAMAGED','INBOUND QUARANTINE','INBOUND UNALLOCABLE')) --BI_6044: excluding inbound labels info TO comply WITH stakeholders request
             THEN f.number_of_assets  else 0
        END) as number_of_assets_others
from dwh.daily_fact_asset_sku_supplier_status f
left join ods_production.product p on p.product_sku=f.product_sku
where fact_date >= '2019-03-01'
group by 1,2,3,4,5,6,7,8,9
order by 3 DESC
)
select *
from oldd
union all
select *
from neww 
;

GRANT SELECT ON dwh.utilization TO tableau;
