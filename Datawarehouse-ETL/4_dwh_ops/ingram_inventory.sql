DROP TABLE IF EXISTS last_ingram_events;

CREATE TEMP TABLE last_ingram_events AS 
WITH take_first_date AS (
    SELECT
        reporting_date,
        order_number,
        serial_number,
        item_number,
        disposition_code,
        status_code,
        source_timestamp,
        asset_serial_number,
        package_serial_number,
        partner_id
    FROM recommerce.ingram_micro_send_order_grading_status
    WHERE
        1 = 1 --only consider first event received per status code
        qualify ROW_NUMBER() OVER(
            PARTITION by order_number,
            disposition_code,
            status_code
            ORDER BY source_timestamp
        ) = 1
),
last_event_of_day AS (
    SELECT
        reporting_date,
        order_number,
        serial_number,
        item_number,
        disposition_code,
        status_code,
        source_timestamp,
        asset_serial_number,
        package_serial_number,
        partner_id
    FROM take_first_date
    WHERE
        1 = 1 --only consider last event of the day
        qualify ROW_NUMBER() OVER(
            PARTITION by reporting_date,
            order_number
            ORDER BY source_timestamp DESC
        ) = 1
)
SELECT
    reporting_date,
    order_number,
    serial_number,
    item_number,
    disposition_code,
    status_code,
    source_timestamp,
    asset_serial_number,
    package_serial_number,
    partner_id,
    LEAD(reporting_date) OVER(
        PARTITION by order_number
        ORDER BY reporting_date
    ) AS next_date
FROM last_event_of_day;

DROP TABLE IF EXISTS ods_operations.ingram_inventory;

CREATE TABLE ods_operations.ingram_inventory AS WITH dates AS (
    SELECT
        datum
    FROM public.dim_dates dd
    WHERE
        datum <= current_date
),
upfill AS (
    SELECT
        datum AS reporting_date,
        e.order_number,
        e.serial_number,
        e.item_number,
        e.disposition_code,
        e.status_code,
        e.source_timestamp,
        e.asset_serial_number,
        e.package_serial_number,
        e.partner_id
    FROM dates d,
        last_ingram_events e --if there is a gap between consecutive events, we must fill with the preceding event for the days in between. 
    WHERE
        datum >= e.source_timestamp :: DATE
        AND (
            datum < e.next_date :: DATE
            OR e.next_date IS NULL
        )
),
ingram_inventory AS (
    SELECT
        reporting_date,
        order_number,
        serial_number,
        item_number,
        e.disposition_code,
        e.status_code,
        source_timestamp,
        asset_serial_number,
        package_serial_number,
        partner_id,
        ikm.in_wh_ind AS is_in_warehouse
    FROM upfill e
    LEFT JOIN staging.ingram_kpis_mapping ikm
        ON e.disposition_code = ikm.disposition_code
        AND e.status_code = ikm.status_code --is_in_warehouse column represents if the asset is with Grover
        --if the is_in_warehouse value is 0, we should take into consideration only first event of corresponding disposition_code, status_code of the asset and ignore the rest
    WHERE
        (
            ikm.in_wh_ind = 1
            OR (
                ikm.in_wh_ind = 0
                AND ikm.status_code IN (
                    'SENT TO REPAIR PARTNER',
                    'OUTBOUND RECOMMERCE ENDED',
                    'OUTBOUND B2B ENDED'
                )
            )
        )
        AND date_part('dow', e.reporting_date) NOT IN ('6', '0')
),
enriching_with_sf AS (
    SELECT
        reporting_date,
        order_number,
        i.serial_number,
        i.item_number,
        disposition_code,
        status_code,
        partner_id,
        i.is_in_warehouse,
        ah.warehouse,
        ah.asset_id,
        ah.asset_status_original,
        ah.category_name,
        ah.subcategory_name,
        ah.asset_name
    FROM ingram_inventory i
    LEFT JOIN MASTER.asset_historical ah
        ON i.serial_number = ah.serial_number
        AND i.reporting_date = dateadd('day', 1, ah."date")
)
SELECT
    reporting_date,
    order_number,
    serial_number,
    item_number,
    disposition_code,
    status_code,
    partner_id,
    is_in_warehouse,
    warehouse,
    asset_id,
    asset_status_original,
    category_name,
    subcategory_name,
    asset_name
FROM enriching_with_sf i
WHERE
    (
        SELECT
            COUNT(unit_srl_no)
        FROM stg_external_apis.ups_nl_oh_inventory unoi
        WHERE
            UPPER(TRIM(unoi.unit_srl_no)) = UPPER(TRIM(i.serial_number))
            AND unoi.report_date :: DATE = i.reporting_date :: DATE
    ) = 0;


GRANT
SELECT
	ON ods_operations.ingram_inventory TO tableau;