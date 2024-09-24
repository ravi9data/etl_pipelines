SELECT customer_id::int,
        sum(CASE
                WHEN asset_status_original = 'WRITTEN OFF DC' THEN 1
                ELSE 0
            END) AS written_off_assets,
        sum(CASE
                WHEN asset_status_original = 'ON LOAN' THEN 1
                ELSE 0
            END) AS has_assets
FROM master.asset a
GROUP BY 1;
