drop table if exists ods_production.customer_allocation_history;
create table ods_production.customer_allocation_history as
select
  a.customer_id,
  count(distinct allocation_id) as allocated_assets,
  min(delivered_at) :: date as first_asset_delivered,
  max(delivered_at) as max_asset_Delivered,
  count(
    distinct case
      when delivered_at is not null then allocation_id
    end
  ) as delivered_allocations,
  count(
    distinct case
      when return_delivery_date is not null then allocation_id
    end
  ) as returned_allocations,
  case
    when count(
      distinct case
        when delivered_at is not null then allocation_id
      end
    ) - count(
      distinct case
        when return_delivery_date is not null then allocation_id
      end
    ) <= 0 then 0
    else count(
      distinct case
        when delivered_at is not null then allocation_id
      end
    ) - count(
      distinct case
        when return_delivery_date is not null then allocation_id
      end
    )
  end as outstanding_assets,
  case
    when count(
      distinct case
        when delivered_at is not null then allocation_id
      end
    ) - count(
      distinct case
        when return_delivery_date is not null then allocation_id
      end
    ) <= 0 then 0
    else sum(
      case
        when delivered_at is not null then ass.initial_price
        else 0
      end
    ) - sum(
      case
        when return_delivery_date is not null then ass.initial_price
        else 0
      end
    )
  end as outstanding_purchase_price,
  count(
    DISTINCT CASE
      WHEN returned_final_condition :: text = 'DAMAGED' :: text THEN allocation_id
      ELSE NULL :: character varying
    END
  ) AS damaged_allocations,
  max(a.updated_at) as updated_at
from ods_production.allocation a
left join ods_production.asset ass on a.asset_id = ass.asset_id
group by
  1;

GRANT SELECT ON ods_production.customer_allocation_history TO tableau;
