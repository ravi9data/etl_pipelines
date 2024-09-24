DROP TABLE IF EXISTS temp_dimensions_ups;
CREATE temp TABLE temp_dimensions_ups as
with 
import_report_pre as (
	select
		sku,
		cast("height" as float) height_ups,
		cast("width"  as float) width_ups,
		cast("length" as float) length_ups,
		cast("weight" as float) weight_ups,
		row_number () over (partition by sku) rn --instead of distinct, take first row 
												 --not possible, but in future there might by different
												 --dimensions per sku. this is safer
 	from staging.stock_ups___raw_data
 	)
select
	sku,
	height_ups,
	width_ups,
	length_ups,
	weight_ups,
	current_date as imported_at,
	current_date as updated_at
from import_report_pre
where rn = 1;


--check if there is a new sku recorded, then insert it
insert into dm_operations.dimensions_ups
select distinct 
	sku, 
	height_ups,
	width_ups,
	length_ups,
	weight_ups,
	current_date as imported_at,
	current_date as updated_at
from temp_dimensions_ups 
where sku not in (select sku from dm_operations.dimensions_ups);

--update if any dimension per sku is updated
update dm_operations.dimensions_ups set height_ups = n.height_ups ,	updated_at = current_date from temp_dimensions_ups n
where dm_operations.dimensions_ups.sku = n.sku and dm_operations.dimensions_ups.height_ups <> n.height_ups;

update dm_operations.dimensions_ups set width_ups = n.width_ups ,updated_at = current_date from temp_dimensions_ups n
where dm_operations.dimensions_ups.sku = n.sku and dm_operations.dimensions_ups.width_ups <> n.width_ups;

update dm_operations.dimensions_ups set length_ups = n.length_ups ,	updated_at = current_date from temp_dimensions_ups n
where dm_operations.dimensions_ups.sku = n.sku and dm_operations.dimensions_ups.length_ups <> n.length_ups;

update dm_operations.dimensions_ups set weight_ups = n.weight_ups ,	updated_at = current_date from temp_dimensions_ups n
where dm_operations.dimensions_ups.sku = n.sku and dm_operations.dimensions_ups.weight_ups <> n.weight_ups;


--PLEASE READ THIS
--
--dimensions in the salesforce are not accurate, and some of them is missing
--in order to cover missing/inaccurate ones, we may use the median of assets in the same category
--even that, this will not give 100% accuracy, but, that is only way to estimate dimensions for each sku
--in general all assets with same product sku, should have same dimensions, or weight.
--but to be on the safe-side, take variant sku
--
--March 17, 2023, dimensions measured by UPS WH implemented, if UPS dimension exists, then that is the most accurate one
drop table if exists dm_operations.dimensions;
create table dm_operations.dimensions as 
with category_dimensions as (
    select category__c,
        --correct unit-wise problems. dimensions to be in centimeters
        case
            when height__c > 10000 then height__c / 1000
            when height__c > 300 then height__c / 10 --an asset cannot be longer than 3 meters
            else height__c
        end as height__c,
        case
            when width__c > 10000 then width__c / 1000
            when width__c > 300 then width__c / 10
            else width__c
        end as width__c,
        case
            when length__c > 10000 then length__c / 1000
            when length__c > 300 then length__c / 10
            else length__c
        end as length__c
    from stg_salesforce.product2
    where greatest(height__c, width__c, length__c) > 0 --only considers assets with a valid dimension
        and nullif(category__c, '') is not null
),
category_dimensions_med as (
    select distinct category__c,
        median(height__c) over (partition by category__c) as category_height,
        median (width__c) over (partition by category__c) as category_width,
        median (length__c) over (partition by category__c) as category_length,
        greatest(category_height, category_width, category_length) as category_longest_dim
    from category_dimensions
),
category_weight as (
    select category__c,
        median (weight__c) med_weight
    from stg_salesforce.product2
    where weight__c > 0
    group by 1
),
stg_fixed as (
    select a.sku_variant__c,
        a.sku_product__c,
        a.name,
        a.brand__c,
        a.category__c,
        a.createddate,
        --mark if dimension or weight is missing
        case
            when least(a.height__c, a.width__c, a.length__c) = 0 --if any is missing
            or greatest (a.height__c, a.width__c, a.length__c) <= 1.01 --or less than 1 centimeter
            or COALESCE(a.height__c::varchar(50), a.width__c::varchar(50), a.length__c::varchar(50), 'a') = 'a'
            then true
            else false
        end as is_dimension_missing,
        case
            when a.weight__c = 0
            or a.weight__c is null then true
            else false
        end as is_weight_missing,
        /*
        case
            when is_dimension_missing then category_longest_dim -- also apply fix on original dimensions
            else greatest(
                case
                    when height__c > 10000 then height__c / 1000
                    when height__c > 300 then height__c / 10
                    else height__c
                end,
                case
                    when width__c > 10000 then width__c / 1000
                    when width__c > 300 then width__c / 10
                    else width__c
                end,
                case
                    when length__c > 10000 then length__c / 1000
                    when length__c > 300 then length__c / 10
                    else length__c
                end
            )
        end longest_dim_pre,
        case
            when longest_dim_pre < 1 then longest_dim_pre * 100
            else longest_dim_pre
        end as longest_dim,
        */
        a.height__c,
        a.width__c,
        a.length__c,
        a.weight__c,
        c.category_height,
        c.category_width,
        c.category_length,
        w.med_weight as category_weight
    from stg_salesforce.product2 a
        left join category_dimensions_med c on a.category__c = c.category__c
        left join category_weight w on a.category__c = w.category__c
),
asset_count as(
    select a.product_sku,
        a.variant_sku,
        a.category_name,
        a.subcategory_name,
        count(
            case
                when a.asset_status_original in ('ON LOAN') then a.asset_id
            end
        ) as assets_on_loan,
        count(
            case
                when a.asset_status_original in (
                    'IN STOCK',
                    'TRANSFERRED TO WH'
                ) then a.asset_id
            end
        ) as assets_in_stock,
        count(
            case
                when a.asset_status_original in (
                    'WARRANTY',
                    'SENT FOR REFURBISHMENT',
                    'RETURNED',
                    'LOCKED DEVICE',
                    'INCOMPLETE',
                    'IN REPAIR',
                    'WAITING FOR REFURBISHMENT'
                ) then a.asset_id
            end
        ) as assets_in_refurbishment,
        count(
            case
                when a.asset_status_original in (
                    'LOST',
                    'LOST SOLVED',
                    'SOLD',
                    'WRITTEN OFF DC',
                    'WRITTEN OFF OPS'
                ) then a.asset_id
            end
        ) as assets_not_available,
        count(a.asset_id) - assets_on_loan - assets_in_stock - assets_in_refurbishment - assets_not_available as other_assets
    from master.asset a
    group by 1, 2, 3, 4
)
select s.sku_variant__c as variant_sku,
	s.sku_product__c as product_sku,
    p.product_name,
    s.createddate as created_at,
    s."name" as variant_name,
    s.brand__c as brand,
    ac.category_name,
    ac.subcategory_name,
    case when u.sku is not null then true else false end as is_ups_dim_available,
    abs(
    coalesce (case when u.height_ups = 0 then 0.2 else u.height_ups end, 
              case when is_dimension_missing 
                   then s.category_height 
                   else s.height__c end)) final_height,
    abs(
    coalesce (case when u.width_ups = 0 then 0.2 else u.width_ups end, 
              case when is_dimension_missing 
                   then s.category_width 
                   else s.width__c end)) final_width,
    abs(
    coalesce (case when u.length_ups = 0 then 0.2 else u.length_ups end, 
              case when is_dimension_missing 
                   then s.category_length 
                   else s.length__c end)) final_length,
    abs(
    coalesce (case when u.weight_ups = 0 then 0.2 else u.weight_ups end,
              case when is_weight_missing 
                   then s.category_weight 
                   else s.weight__c end)) final_weight,
    s.height__c as original_height,
    s.width__c as original_width,
    s.length__c as original_length,
    s.weight__c as original_weight,
    s.is_dimension_missing,
    s.is_weight_missing,
    greatest( final_height , final_width , final_length ) as longest_dim,
    s.category_height,
    s.category_width,
    s.category_length,
    s.category_weight,
    ac.assets_on_loan,
    ac.assets_in_stock,
    ac.assets_in_refurbishment,
    ac.other_assets,
    case when final_weight>0 and final_weight<=1 then '0Kg-1Kg'
	 	when final_weight>1 and final_weight<=2 then '1Kg-2Kg'
	 	when final_weight>2 and final_weight<=3 then '2Kg-3Kg'
		when final_weight>3 and final_weight<=4 then '3Kg-4Kg'
		when final_weight>4 and final_weight<=5 then '4Kg-5Kg'
		when final_weight>5 and final_weight<=6 then '5Kg-6Kg'
		when final_weight>6 and final_weight<=7 then '6Kg-7Kg'
		when final_weight>7 and final_weight<=8 then '7Kg-8Kg'
		when final_weight>8 and final_weight<=9 then '8Kg-9Kg'
		when final_weight>9 and final_weight<=10 then '9Kg-10Kg'
		when final_weight>10 and final_weight<=12 then '10Kg-12Kg'
		when final_weight>12 and final_weight<=14 then '12Kg-14Kg'
		when final_weight>14 and final_weight<=16 then '14Kg-16Kg'
		when final_weight>16 and final_weight<=18 then '16Kg-18Kg'
		when final_weight>18 and final_weight<=20 then '18Kg-20Kg'
		when final_weight>20 and final_weight<=22 then '20Kg-22Kg'
		when final_weight>22 and final_weight<=25 then '22Kg-25Kg'
		when final_weight>25 and final_weight<=28 then '25Kg-28Kg'
		when final_weight>28 and final_weight<=31 then '28Kg-31Kg'
		when final_weight>31 and final_weight<=34 then '31Kg-34Kg'
		when final_weight>34 and final_weight<=36 then '34Kg-36Kg'
		when final_weight>36 and final_weight<=38 then '36Kg-38Kg'
		when final_weight>38 and final_weight<=40 then '38Kg-40Kg'
		when final_weight>40 and final_weight<=42 then '40Kg-42Kg'
		when final_weight>42 and final_weight<=44 then '42Kg-44Kg'
		when final_weight>44 and final_weight<=46 then '44Kg-46Kg'
		when final_weight>46 and final_weight<=48 then '46Kg-48Kg'
		when final_weight>50 and final_weight<=55 then '50Kg-55Kg'
		when final_weight>55 and final_weight<=60 then '55Kg-60Kg'
		when final_weight>60 and final_weight<=65 then '60Kg-65Kg'
		when final_weight>65 and final_weight<=70 then '65Kg-70Kg'
		when final_weight>70 then '70Kg+' end as weight_bucket
from stg_fixed s
    left join asset_count ac on ac.variant_sku = s.sku_variant__c and s.sku_product__c = ac.product_sku
    left join ods_production.product p on s.sku_product__c=p.product_sku
    left join dm_operations.dimensions_ups u on s.sku_variant__c = u.sku;

GRANT SELECT ON dm_operations.dimensions TO hightouch_pricing;
GRANT SELECT ON dm_operations.dimensions TO GROUP pricing;
GRANT SELECT ON dm_operations.dimensions TO redash_pricing;
GRANT SELECT ON dm_operations.dimensions TO tableau;
GRANT SELECT ON dm_operations.dimensions TO GROUP recommerce;
