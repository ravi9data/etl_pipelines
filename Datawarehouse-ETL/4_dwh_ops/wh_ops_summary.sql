DROP TABLE IF EXISTS dm_operations.wh_ops_summary;

CREATE TABLE dm_operations.wh_ops_summary AS WITH dates AS (
    SELECT
        datum
    FROM public.dim_dates
    WHERE
        datum >= current_date - 30 --daily last 30 days
        OR (
            datum >= current_date - (12 * 7) --last 12 weeks
            AND date_part(dayofweek, datum) = 5
        ) --only fridays
),
al_histo AS (
    SELECT
        DATE,
        allocation_id,
        asset_id,
        allocation_status_original,
        allocated_at,
        shipment_label_created_at,
        return_shipment_at,
        return_delivery_date,
        failed_delivery_at,
        is_last_allocation_per_asset
    FROM MASTER.allocation_historical
    WHERE
        DATE IN (
            SELECT
                datum
            FROM dates
        )
),
as_histo AS (
    SELECT
        DATE,
        asset_id,
        serial_number,
        asset_status_original,
        customer_id,
        subscription_id
    FROM MASTER.asset_historical
    WHERE
        DATE IN (
            SELECT
                datum
            FROM dates
        ) --incomplete, warrant, in repair, locked device (sperrlagger)
),
im_events AS (
    SELECT
        serial_number,
        source_timestamp AS first_event
    FROM recommerce.ingram_micro_send_order_grading_status im
    WHERE
        im.serial_number IN (
            SELECT
                serial_number
            FROM as_histo
        )
),
--first reduce, then join
historical AS (
    SELECT
        a.date,
        a.allocation_id,
        a.asset_id,
        a.allocation_status_original,
        a.allocated_at,
        a.shipment_label_created_at,
        a.return_shipment_at,
        a.return_delivery_date,
        a.failed_delivery_at,
        a.is_last_allocation_per_asset,
        t.serial_number,
        t.asset_status_original,
        t.customer_id,
        t.subscription_id,
        (
            SELECT
                MIN(i.first_event)
            FROM im_events i
            WHERE
                i.serial_number = t.serial_number
                AND i.first_event > a.return_delivery_date
        ) first_im_event
    FROM al_histo a
    LEFT JOIN as_histo t
        ON a.date = t.date
        AND a.asset_id = t.asset_id
),
counts AS (
    SELECT
        DATE,
        COUNT(
            CASE
                WHEN failed_delivery_at IS NOT NULL
                AND asset_status_original = 'RETURNED'
                AND return_shipment_at IS NULL
                AND is_last_allocation_per_asset
                    THEN allocation_id
            END
        ) failed_not_scanned,
        COUNT(
            CASE
                WHEN return_shipment_at IS NOT NULL
                AND allocation_status_original = 'IN TRANSIT'
                AND asset_status_original = 'ON LOAN'
                AND return_delivery_date IS NULL
                AND is_last_allocation_per_asset
                    THEN allocation_id
            END
        ) returns_in_transit,
        COUNT(
            CASE
                WHEN allocation_status_original = 'READY TO SHIP'
                AND shipment_label_created_at IS NULL
                AND is_last_allocation_per_asset
                    THEN allocation_id
            END
        ) ready_to_ship_backlog,
        COUNT(
            CASE
                WHEN return_delivery_date IS NOT NULL
                AND allocation_status_original = 'IN TRANSIT'
                AND asset_status_original = 'ON LOAN'
                AND (
                    first_im_event IS NULL --not scanned yet
                    OR (
                        first_im_event > return_delivery_date
                        AND first_im_event > DATE
                    ) --scanned later , as of snapshot date it was not scanned
                )
                AND is_last_allocation_per_asset
                    THEN allocation_id
            END
        ) returned_not_scanned,
        COUNT(
            DISTINCT CASE
                WHEN asset_status_original = 'RETURNED'
                AND customer_id IS NOT NULL
                AND subscription_id IS NOT NULL
                    THEN asset_id
            END
        ) assets_in_refurb,
        COUNT(
            DISTINCT CASE
                WHEN asset_status_original IN (
                    'INCOMPLETE',
                    'WARRANTY',
                    'IN REPAIR',
                    'LOCKED DEVICE'
                )
                    THEN asset_id
            END
        ) sperrlagger,
        COUNT(
            DISTINCT CASE
                WHEN asset_status_original IN ('SELLING')
                    THEN asset_id
            END
        ) selling
    FROM historical
    GROUP BY 1
),
pending_allocations AS (
    SELECT
        datum,
        SUM(
            CASE
                WHEN store_short = 'Grover'
                    THEN pending_allocations
            END
        SUM(
            CASE
                WHEN store_short = 'Grover International'
                    THEN pending_allocations
            END
        SUM(
            CASE
                WHEN store_short = 'Partners Online'
                    THEN pending_allocations
            END
        ) pa_partners
    FROM ods_production.inventory_reservation_pending_historical
    WHERE
        hours = 13
        AND datum IN (
            SELECT
                datum
            FROM dates
        )
        AND datum <> current_date --exclude today
    GROUP BY 1
    UNION
    --current_date
    SELECT
        current_date AS datum,
        COUNT(
            CASE
                WHEN store_short = 'Grover'
                    THEN reservation_id
            END
        COUNT(
            CASE
                WHEN store_short = 'Grover International'
                    THEN reservation_id
            END
        COUNT(
            CASE
                WHEN store_short = 'Partners Online'
                    THEN reservation_id
            END
        ) pa_partners
    FROM ods_production.inventory_reservation_pending
    GROUP BY 1
)
SELECT
    c.*,
    p.pa_partners
FROM counts c
LEFT JOIN pending_allocations p
    ON c.date = p.datum;


GRANT SELECT ON dm_operations.wh_ops_summary TO tableau;