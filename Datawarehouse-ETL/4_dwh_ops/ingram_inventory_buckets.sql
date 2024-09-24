DROP TABLE IF EXISTS dm_operations.ingram_inventory_buckets;

CREATE TABLE dm_operations.ingram_inventory_buckets AS WITH rd AS (
    SELECT
        reporting_date,
        disposition_code,
        status_code,
    -- in grading
        CASE
            WHEN disposition_code = 'GRADING'
                THEN CASE
                WHEN status_code IN (
                    'LITE GRADING START',
                    'LITE GRADING FINISH'
                )
                    THEN 'In Grading-Repair'
                WHEN status_code IN (
                    'WAITING FOR GRADING',
                    'GRADING START',
                    'GRADING FINISHED'
                )
                    THEN 'In Grading-Normal'
            END 
    -- in refurbishment
            WHEN disposition_code = 'REFURBISHMENT'
                THEN CASE
                WHEN status_code = 'WAITING FOR REFURBISHMENT'
                    THEN 'In Refurbishment-Waiting for Refurbishment'
                ELSE 'In Refurbishment-In Process'
            END -- in locked
            WHEN disposition_code = 'LOCKED'
            AND status_code = 'AVAILABLE, LOCKED'
                THEN 'In Locked-Locked' 
    -- in clarification
            WHEN disposition_code = 'CLARIFICATION'
                THEN CASE
                WHEN status_code IN (
                    'REPAIR PREALERT MISSING',
                    'REPAIR PREALERT RECEIVED'
                )
                    THEN 'In Clarification-Repair'
                WHEN status_code IN ('CLARIFICATION START', 'OTHER')
                    THEN 'In Clarification-Missing Reason'
                WHEN status_code IN ('CLARIFICATION END')
                    THEN 'In Clarification-Move to Received'
                ELSE 'In Clarification-Tickets'
            END 
    -- in quarantine
            WHEN disposition_code = 'QUARANTINE'
                THEN CASE
                WHEN status_code = 'QUARANTINE INVESTIGATION'
                    THEN 'In Quarantine-Investigation'
                WHEN status_code IN (
                    'LOST IN WAREHOUSE',
                    'LOST IN TRANSIT'
                )
                    THEN 'In Quarantine-Lost'
                WHEN status_code = 'BOOKING ERROR'
                    THEN 'In Quarantine-Booking Error'
            END
            WHEN disposition_code = 'CLOSED'
            AND status_code = 'LOST'
                THEN 'In Quarantine-Lost' 
    -- goods in 
            WHEN disposition_code = 'GOODS_IN'
            AND (
                status_code IN ('RETURNED RETURN', 'SCANNED AT WAREHOUSE')
                OR status_code IS NULL
            )
                THEN 'Goods In-Started'
            WHEN disposition_code = 'RETURNED'
            AND status_code = 'RETURN RECEIVED'
                THEN 'Goods In-Finished' 
    -- in channelling
            WHEN disposition_code = 'CHANNELING'
            AND status_code = 'AWAITING DECISION'
                THEN 'In Channeling-Channeling' 
    -- to transfer 
            WHEN status_code = 'WAITING TO BE SENT TO WAREHOUSE'
                THEN 'To Transfer-Available' --to be split as "healty" and "to be fixed" later
            WHEN disposition_code = 'STOCK'
            AND status_code = 'ALLOCATED WAREHOUSE'
                THEN 'To Transfer-To WH Transfer' 
    -- in repair
            WHEN disposition_code = 'REPAIR'
            AND status_code IN (
                'WAITING TO BE SENT REPAIR',
                'AVAILABLE, REPAIR'
            )
                THEN 'In Repair-In the WH'
            WHEN disposition_code = 'GOODS_OUT'
            AND status_code = 'SENT TO REPAIR PARTNER'
                THEN 'In Repair-At Repair Partner' --this will never show up (to be discussed later)
            WHEN (
                disposition_code = 'RETURNED REPAIR'
                AND status_code = 'REPAIR RECEIVED'
            )
            OR (
                disposition_code = 'GOODS_IN'
                AND status_code = 'RETURNED REPAIR'
            )
                THEN 'In Repair-Back from Repair Partner' 
    -- in recommerce
            WHEN (
                disposition_code = 'RECOMMERCE'
                AND status_code IN (
                    'AVAILABLE, RECOMMERCE',
                    'WAITING TO BE SENT RECOMMERCE',
                    'ALLOCATED RECOMMERCE'
                )
            )
            OR (
                disposition_code = 'GOODS_OUT'
                AND status_code = 'SENT TO CONSIGNMENT PARTNER'
            )
                THEN 'In Recommerce-Available'
            WHEN disposition_code = 'RECOMMERCE'
            AND status_code = 'WAITING TO BE SENT CONSIGNMENT'
                THEN 'In Recommerce-Consignment in Prep.'
            WHEN disposition_code = 'RECOMMERCE'
            AND status_code = 'WAITING TO BE SENT B2B'
                THEN 'In Recommerce-B2B Sells in Prep.' 
    -- in stock
            WHEN disposition_code = 'STOCK'
                THEN CASE
                WHEN status_code = 'ALLOCATED'
                    THEN 'In Stock-Allocated'
                WHEN status_code = 'AVAILABLE, GROVER'
                    THEN 'In Stock-Available'
            END
            ELSE 'Not Mapped'
        END inventory_bucket
    FROM ods_operations.ingram_inventory --previously dm_recommerce.ingram_inventory 
    where is_in_warehouse = 1
)
SELECT
    reporting_date,
    inventory_bucket,
    split_part(inventory_bucket, '-', 1) main_bucket,
    split_part(inventory_bucket, '-', 2) sub_bucket,
    disposition_code,
    status_code,
    COUNT(1) total_assets
FROM rd
GROUP BY 1, 2, 3, 4, 5, 6;


GRANT SELECT ON dm_operations.ingram_inventory_buckets TO tableau;