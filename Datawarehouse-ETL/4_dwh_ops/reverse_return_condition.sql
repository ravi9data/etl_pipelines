DROP TABLE IF EXISTS dm_operations.reverse_return_condition;

CREATE TABLE dm_operations.reverse_return_condition AS
SELECT
    'initial_condition' AS graph_type,
    date_trunc('week', return_delivery_date) fact_date,
    initial_condition AS asset_status,
    COUNT(allocation_id) total_returns
FROM ods_operations.allocation_return_condition
GROUP BY 1, 2, 3 --
UNION ALL
--
SELECT
    'final_condition' AS graph_type,
    date_trunc('week', return_delivery_date) fact_date,
    final_condition AS asset_status,
    COUNT(allocation_id) total_returns
FROM ods_operations.allocation_return_condition
GROUP BY 1, 2, 3;


GRANT SELECT ON dm_operations.reverse_return_condition TO tableau;