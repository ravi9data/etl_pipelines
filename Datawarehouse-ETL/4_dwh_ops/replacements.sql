drop table if exists dm_operations.replacements;
create table dm_operations.replacements as with r as (
    select o.allocation_id,
        o.asset_id,
        a.residual_value_market_price,
        o.serial_number,
        --to identify if the customer still keeps the old asset
        --then we need to filter only delivered shipments out.
        case
            when o.delivered_at is not null then 'Delivered'
            else 'Not Delivered'
        end as is_delivered,
        o.customer_id,
        o.allocation_status_original,
        o.asset_status,
        o.is_recirculated,
        o.allocated_at,
        o.shipment_at,
        o.delivered_at,
        o.replacement_date,
        n.shipment_at replaced_shipment_at,
        n.delivered_at replaced_delivered_at,
        o.replacement_reason,
        o.replaced_by,
        o.return_shipment_id,
        o.return_shipment_tracking_number,
        o.return_shipment_label_created_at,
        o.return_shipment_at,
        o.return_delivery_date,
        o.return_delivery_date_old
    from ods_production.allocation o
        left join master.asset a on o.asset_id = a.asset_id
        left join ods_production.allocation n on o.replaced_by = n.allocation_id
    where o.replacement_date is not null -- it should be replaced obviously
),
--check when asset back in the stock
--since for some allocation return timestamps are not available 
--even if they are back in the wh.
--classify each day per shipment date, so we'll be able to detect if asset has returned 
wemalo as (
    select w.seriennummer,
        w.reporting_date,
        case
            when r.shipment_at < w.reporting_date then row_number() over (
                partition by w.seriennummer,
                case
                    when r.shipment_at > w.reporting_date then 1
                    else null
                end
                order by w.reporting_date asc
            )
        end rn
    from stg_external_apis.wemalo_inventory w
        left join r on r.serial_number = w.seriennummer
),
wemalo_f as (
    select seriennummer,
        reporting_date,
        rn
    from wemalo
    where rn = 1
)
select r.allocation_id,
    r.asset_id,
    r.residual_value_market_price,
    r.serial_number,
    r.is_delivered,
    r.customer_id,
    r.allocation_status_original,
    r.asset_status,
    r.is_recirculated,
    r.allocated_at,
    r.shipment_at,
    r.delivered_at,
    r.replacement_date,
    r.replaced_shipment_at,
    r.replaced_delivered_at,
    r.replacement_reason,
    r.replaced_by,
    r.return_shipment_id,
    r.return_shipment_tracking_number,
    r.return_shipment_label_created_at,
    r.return_shipment_at,
    coalesce (
        coalesce(
            r.return_delivery_date,
            r.return_delivery_date_old
        ),
        case
            when f.reporting_date > r.replacement_date then f.reporting_date
            else null
        end
    ) return_delivery
from r
    left join wemalo_f f on r.serial_number = f.seriennummer;

GRANT SELECT ON dm_operations.replacements TO tableau;
