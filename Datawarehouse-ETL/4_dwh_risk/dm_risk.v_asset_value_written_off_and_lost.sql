DROP VIEW IF EXISTS dm_risk.v_asset_value_written_off_and_lost; 
CREATE VIEW dm_risk.v_asset_value_written_off_and_lost AS
--get only 'WRITTEN OFF DC' and 'LOST' informations as they are not considered in ods_spv_historical.asset_market_value
WITH assets AS (
    SELECT
        asset_id,
        product_sku,
        purchased_date,
        MAX(purchased_date) OVER (PARTITION BY product_sku) AS last_sku_purchase,
        initial_price
    FROM ods_production.asset
    WHERE initial_price IS NOT NULL 
    	AND initial_price > 0 
    	AND asset_status_original IN ('WRITTEN OFF DC','LOST')
	),

    avg_initial_price AS (
    SELECT
        product_sku,
        AVG(initial_price) AS initial_price
    FROM assets
    WHERE purchased_date = last_sku_purchase
    GROUP BY 1
	),

	--take only one row in case of bugs like product_sku = 'GRB224P12323' on '2024-01-31'
	--take price from previous month to be consistent with ods_production.subscription_assets logic
    price_per_condition AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_sku ORDER BY COALESCE(neu_price, -1) DESC) AS rn --COALESCE(neu_price, -1) as I don't want to exclude null in case there is nothing else there per date, but some of the other columns might be used
        FROM ods_spv_historical.price_per_condition
        WHERE reporting_date::DATE = DATE_TRUNC('month', current_date) - 1
    )

    SELECT DISTINCT
        ss.asset_id,
        ss.asset_status_original,
        ss.subscription_id,
        CASE
            /*###############
            USED AND IN STOCK FOR 6M
            #####################*/
            WHEN (ss.asset_condition_spv = 'USED')
                AND ss.last_allocation_days_in_stock >= (6 * 30)
                AND (c.m_since_used_price_standardized is null or COALESCE(c.m_since_used_price_standardized,0) > 6)
                THEN 0
            /*###############
            USED and USED price available
            #####################*/
            WHEN (ss.asset_condition_spv = 'USED')
                AND c.used_price_standardized IS NOT NULL
                THEN c.used_price_standardized
            /*###############
             NEW was never delivered -
             new price if available
             else greatest from agan price and depreciated purchase/price & rrp
             #####################*/
            WHEN ss.asset_condition_spv = 'NEW'
                AND COALESCE(ss.delivered_allocations, 0) = 0
                AND COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
                                                                (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) *
                                                                COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
                THEN COALESCE(c.new_price_standardized, GREATEST(c.agan_price_standardized,
                                                                 (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) *
                                                                 COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
            /*###############
            IN STOCK FOR 6m
            #####################*/
            WHEN (ss.asset_condition_spv = 'AGAN' )
                --               	 OR ss.asset_condition_spv = 'NEW')
                --                	 AND COALESCE(ss.delivered_allocations, 0) > 0
                AND ss.last_allocation_days_in_stock >= (6 * 30)
                AND (c.m_since_agan_price_standardized is null or COALESCE(c.m_since_agan_price_standardized,0) > 6)
                THEN 0
            /*###############
            AGAN, greatest of used and initial price depreciated
            #####################*/
            WHEN (ss.asset_condition_spv = 'AGAN'
                OR ss.asset_condition_spv = 'NEW'
                      AND COALESCE(ss.delivered_allocations, 0) > 0)
                AND COALESCE(c.agan_price_standardized,
                             GREATEST(c.used_price_standardized,
                                      COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price))) IS NOT NULL
                THEN COALESCE(c.agan_price_standardized,
                              GREATEST(c.used_price_standardized, (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date::date) / 30)) *
                                                                  COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)))
            WHEN ((1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date) / 30)) *
                  COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)) <= 0
                THEN 0
            ELSE (1 - 0.03 * ((date_trunc('month', current_date-1)::date - ss.purchased_date) / 30)) * COALESCE(ss.initial_price, ss.amount_rrp, e.initial_price)
            END AS residual_value_market_price_written_off_and_lost
    FROM master.asset ss
        INNER JOIN assets a ON a.asset_id = ss.asset_id
        LEFT JOIN price_per_condition c ON c.product_sku = ss.product_sku AND rn = 1
        LEFT JOIN avg_initial_price e ON e.product_sku = ss.product_sku
    WITH NO SCHEMA BINDING;

