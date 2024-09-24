DROP VIEW IF EXISTS dm_catman.v_mietkauf_subscriptions;
CREATE VIEW dm_catman.v_mietkauf_subscriptions AS
SELECT a.subscription_id,
       a.start_date,
       a.cancellation_reason_new,
       a.paid_subscriptions,
       a.cancellation_date,
       a.subcategory_name,
       a.category_name,
       a.country_name,
       a.product_name,
       CASE WHEN a.store_commercial ILIKE '%B2B%' THEN 'B2B'
            WHEN a.store_commercial ILIKE '%Partnerships%' THEN 'Retail'
            ELSE 'B2C'
           END AS store_commercial_split,
       CASE
           WHEN a.customer_type IS NULL AND store_commercial_split ILIKE '%B2B%' THEN 'B2B'
           WHEN a.customer_type IS NULL AND store_commercial_split ILIKE '%B2C%' THEN 'B2C'
           ELSE a.customer_type
           END AS customer_type,
       c.purchased_date,
       b.allocation_id,
       u.full_name AS account_owner,
       COALESCE(u.segment, 'Self Service') AS segment,
       c.asset_id,
       a.asset_recirculation_status,
       a.subscription_plan,
       a.buyout_disabled,
       a.buyout_disabled_at,
       a.buyout_disabled_reason,
       a.subscription_value 
FROM master.subscription a
         LEFT JOIN master.allocation b on a.subscription_id = b.subscription_id
         LEFT JOIN master.asset c on c.asset_id = b.asset_id
         LEFT JOIN ods_b2b.account d
                   on a.customer_id = d.customer_id
         LEFT JOIN ods_b2b."user" u
                   on d.account_owner = u.user_id
        WITH NO SCHEMA BINDING;
