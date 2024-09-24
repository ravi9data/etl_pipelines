drop table if exists ods_production.mietkauf_prices;
create table ods_production.mietkauf_prices 
as 
with b as(
select
	max(datum) as datum,
	product_sku
where
price_pp is not null
GROUP by
product_sku
),
c as (
select
	b.product_sku,
	case
		when datediff(MONTH, b.datum, current_date) < 7 and datediff(MONTH, b.datum, current_date) > -1 THEN 1
		ELSE 0
	END as cond
from b
),
d as (
select DISTINCT 
	m.product_sku,
	last_value(price_spv ignore nulls) over(partition by m.product_sku order by datum ASC rows between unbounded preceding and unbounded following) as last_spv_price,
	last_value(price_mm ignore nulls) over(partition by m.product_sku order by datum ASC rows between unbounded preceding and unbounded following) as last_mm_price,
	last_value(price_mm_value_3_weeks ignore nulls) over(partition by m.product_sku order by datum ASC rows between unbounded preceding and unbounded following) as last_mm_value_3_weeks_price,
	last_value(price_rrp ignore nulls) over(partition by m.product_sku order by datum ASC rows between unbounded preceding and unbounded following) as last_rrp_price,
	case
        when cond = '1' then last_value(price_pp ignore nulls) over(partition by m.product_sku order by datum ASC rows between unbounded preceding and unbounded following)
        else 0
	end as last_pp_price
left join c on m.product_sku = c.product_sku
)
select
  product_sku,
  last_spv_price,
  last_mm_price,
  last_mm_value_3_weeks_price,
  last_rrp_price,
  last_pp_price,
  case
    when last_pp_price > COALESCE(
      (
        case
          when last_mm_value_3_weeks_price > last_spv_price then last_mm_value_3_weeks_price
          when last_spv_price > last_mm_value_3_weeks_price then least((last_mm_value_3_weeks_price * 1.1), last_spv_price)
          when COALESCE(last_spv_price, 0) > 0 then last_spv_price
          when COALESCE(last_mm_value_3_weeks_price, 0) > 0 then last_mm_value_3_weeks_price
          else null
        end
      ),
      0
    )
    and last_pp_price <> 0 then last_pp_price
    else (
      case
        when last_mm_value_3_weeks_price > last_spv_price then last_mm_value_3_weeks_price
        when last_spv_price > last_mm_value_3_weeks_price then least((last_mm_value_3_weeks_price * 1.1), last_spv_price)
        when COALESCE(last_spv_price, 0) > 0 then last_spv_price
        when COALESCE(last_mm_value_3_weeks_price, 0) > 0 then last_mm_value_3_weeks_price
        else null
      end
    )
  end as mkp
from d;