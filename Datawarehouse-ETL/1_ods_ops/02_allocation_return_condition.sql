DROP TABLE IF EXISTS ods_operations.allocation_return_condition;

CREATE TABLE ods_operations.allocation_return_condition AS WITH al AS (
    SELECT
        allocation_id,
        asset_id,
        allocation_status_original,
        allocated_at,
        return_delivery_date,
        LEAD(allocated_at) OVER (
            PARTITION by asset_id
            ORDER BY allocated_at
        ) AS next_allocated_at,
        returned_final_condition
    FROM ods_production.allocation 
),
hist AS (
    SELECT
        assetid,
        newvalue,
        CASE
            WHEN newvalue IN ('RETURNED', 'IN STOCK', 'TRANSFERRED TO WH')
                THEN newvalue
            WHEN newvalue IN (
                'IN REPAIR',
                'IRREPARABLE',
                'LOCKED DEVICE',
                'INCOMPLETE'
            )
                THEN 'PROBLEM'
            ELSE 'OTHER'
        END AS status_group,
        createddate
    FROM stg_salesforce.asset_history
    WHERE
        field = 'Status' 
),
status_updates AS (
    SELECT
        a.allocation_id,
        a.asset_id,
        a.allocation_status_original,
        a.allocated_at,
        a.return_delivery_date,
        a.next_allocated_at,
        h.newvalue,
        h.createddate,
        h.status_group,
        ROW_NUMBER () OVER (
            PARTITION by a.allocation_id,
            a.asset_id,
            h.status_group
            ORDER BY h.createddate DESC
        ) rn
    FROM al a
    LEFT JOIN hist h
        ON h.assetid = a.asset_id
        AND a.allocated_at < h.createddate
        AND COALESCE (a.next_allocated_at, '2099-01-01') > h.createddate
),
set_initial_final AS (
    SELECT
        allocation_id,
        asset_id,
        return_delivery_date,
        MIN(
            CASE
                WHEN status_group = 'RETURNED'
                    THEN createddate
            END
        ) returned_status_date,
        MAX(
            CASE
                WHEN status_group = 'PROBLEM'
                AND rn = 1
                    THEN createddate
            END
        ) problem_date,
        MAX(
            CASE
                WHEN status_group = 'PROBLEM'
                AND rn = 1
                    THEN newvalue
            END
        ) problem_condition,
        MAX(
            CASE
                WHEN status_group = 'OTHER'
                AND rn = 1
                    THEN createddate
            END
        ) other_date,
        MAX(
            CASE
                WHEN status_group = 'OTHER'
                AND rn = 1
                    THEN newvalue
            END
        ) other_condition,
        MAX(
            CASE
                WHEN status_group IN ('IN STOCK', 'TRANSFERRED TO WH')
                AND rn = 1
                    THEN createddate
            END
        ) in_stock_date
    FROM status_updates
    GROUP BY 1, 2, 3
)
SELECT
    allocation_id,
    asset_id,
    return_delivery_date,
    returned_status_date,
    problem_date,
    problem_condition,
    in_stock_date,
    other_date,
    CASE
        WHEN problem_condition IS NULL
        AND COALESCE (returned_status_date, in_stock_date) IS NOT NULL
            THEN 'AGAN'
        WHEN problem_condition IS NOT NULL
            THEN problem_condition
    END AS initial_condition,
    CASE
        WHEN problem_condition IS NOT NULL
        AND in_stock_date > problem_date
            THEN 'AGAN'
        WHEN problem_condition IS NOT NULL
        AND other_date > problem_date
            THEN 'OTHER'
        ELSE initial_condition
    END AS final_condition
FROM set_initial_final
WHERE
    return_delivery_date IS NOT NULL;