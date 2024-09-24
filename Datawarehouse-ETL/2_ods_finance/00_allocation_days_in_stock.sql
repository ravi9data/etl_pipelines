drop table if exists ods_production.allocation_days_in_stock;
create table ods_production.allocation_days_in_stock as
Select	
	aa.allocation_id,
	aa.asset_id,
        CASE
            WHEN (
            CASE
                WHEN aa.is_last_allocation_per_asset THEN COALESCE(ast.sold_date,
                CASE
                    WHEN aa.status_new = 'LOST'::text THEN ast.updated_date
                    ELSE NULL::timestamp
                END, 'now'::text::date::timestamp)::date
                ELSE lead(aa.shipment_at) OVER (PARTITION BY aa.asset_id ORDER BY aa.allocated_at)::date
            END - COALESCE(aa.return_delivery_date, aa.in_stock_at)::date) < 0 
             THEN NULL::integer
            ELSE
            CASE
                WHEN aa.is_last_allocation_per_asset THEN COALESCE(ast.sold_date,
                CASE
                    WHEN ast.asset_status_grouped = 'LOST'::text THEN ast.updated_date
                    ELSE NULL::timestamp 
                END, 'now'::text::date::timestamp)::date
                ELSE lead(aa.shipment_at) OVER (PARTITION BY aa.asset_id ORDER BY aa.allocated_at)::date
            END - COALESCE(aa.return_delivery_date, aa.in_stock_at)::date
        END AS days_in_stock
   from ods_production.allocation aa
   left join ods_production.asset ast on ast.asset_id=aa.asset_id
   left join ods_production.subscription s on s.subscription_id=aa.subscription_id
   