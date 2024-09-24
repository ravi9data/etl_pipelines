truncate table dm_recommerce.self_service_4r;

insert into dm_recommerce.self_service_4r
with wemalo as(
    select
        *
    from
        (
            select
                seriennummer as serial_number,
                asset_id,
                spl_position as position,
                stellplatz,
                reporting_date,
                row_number() over(
                    partition by seriennummer
                    order by
                        reporting_date desc
                ) as last_pos
            from
                dm_recommerce.wemalo_inventory_daily_movement
            where
                spl_position is not null
        )
    where
        last_pos = 1
),
latest_order as(
    select
        last_allo.*,
        s.order_id as last_order_id
    from(
            select
                *
            from
                (
                    select
                        row_number() over(
                            partition by asset_id
                            ORDER BY
                                allocated_at desc
                        ) rn,
                        a.allocation_id,
                        a.asset_id,
                        a.allocated_at as last_allocation_date,
                        a.subscription_id as last_subscription_id
                    from
                        master.allocation a
                )
            where
                rn = 1
        ) as last_allo --a.asset_id ='02i580000061QcKAAU'
        left join master.subscription s on s.subscription_id = last_allo.last_subscription_id
),
damaged_assets as(
    select
        dam.serialnumber,
        json_extract_path_text(
            json_extract_array_element_text(dam."options", 0),
            'label'
        ) as optical_attr,
        json_extract_path_text(
            json_extract_array_element_text(dam."attributes", 1),
            'displayGerman'
        ) as functional_attr,
        regexp_replace(
            REGEXP_REPLACE(
                json_extract_path_text(
                    json_extract_array_element_text(dam."attributes", 1),
                    'notice'
                ),
                '<[^>]*>',
                ''
            ),
            '(\\\\r|\\\\n|\\\\t)',
            ''
        ) as functional_descr,
        dam.event_timestamp as times
    from
        stg_kafka_events_full.stream_wms_wemalo_register_refurbishment dam
    where
        times = (
            select
                max(dam2.event_timestamp)
            from
                (
                    select
                        *
                    from
                        stg_kafka_events_full.stream_wms_wemalo_register_refurbishment
                    where
                        cell SIMILAR TO '%(Spl-KD-1|Spl-KD-0)%'
                ) dam2
            where
                dam2.serialnumber = dam.serialnumber
        )
),
repairs as(
    select
        serialnumber,
        listagg(distinct cell, '|') as liste
    from
        stg_kafka_events_full.stream_wms_wemalo_register_refurbishment
    where
        cell SIMILAR TO '%(Spl-KD-1|Spl-KD-0)%'
    group by
        serialnumber
)
select
    w.serial_number,
    w.stellplatz,
    w.position,
    a.asset_id,
    a.asset_status_original,
    a.asset_name,
    a.category_name,
    a.subcategory_name,
    a.variant_sku,
    count(
        case
            when a.asset_status_original = 'IN STOCK' then w.serial_number
        end
    ) over(partition by a.variant_sku) as assets_in_stock_for_each_variant_sku,
    count(
        case
            when a.asset_status_original = 'ON LOAN' then w.serial_number
        end
    ) over(partition by a.variant_sku) as assets_on_loan_for_each_variant_sku,
    count(w.serial_number) over(partition by a.variant_sku) as assets_for_each_variant_sku,
    a.product_sku,
    a.brand,
    a.residual_value_market_price,
    a.last_month_residual_value_market_price,
    a.last_market_valuation,
    a.subscription_revenue,
    a.purchased_date,
    a.supplier,
    a.initial_price,
    a.asset_condition,
    a.asset_condition_spv,
    a.dpd_bucket,
    a.total_allocations_per_asset,
    round(
        (
            24 - months_between(current_date, a.purchased_date)
        ),
        0
    ) as lifetime_24,
    w.reporting_date as date_of_last_movement,
    da.optical_attr,
    da.functional_attr,
    da.functional_descr,
    regexp_count(r.liste, '[|]') + 1 as total_repair,
    su.utilization,
    lo.last_order_id
from
    wemalo w
    left join master.asset a on a.serial_number = w.serial_number
    left join latest_order lo on lo.asset_id = a.asset_id
    left join damaged_assets da on da.serialnumber = a.serial_number
    left join repairs r on r.serialnumber = a.serial_number
    left join dm_recommerce.sku_utilization su on a.product_sku = su.product_sku;
