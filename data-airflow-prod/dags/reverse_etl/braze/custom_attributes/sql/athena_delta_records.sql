WITH deduped_history AS (
SELECT  external_id,
        "_update_existing_only",
        customer_label,
        profile_status,
        company_status,
        customer_type,
        company_created_at,
        trust_type,
        company_type,
        qualified_for_review_invitation,
        lifetime_rented_product,
        lifetime_rented_category,
        lifetime_rented_subcategory,
        lifetime_rented_brand,
        lifetime_rented_sku,
        lifetime_rental_plan,
        lifetime_voucher_redeemed,
        rfm_segment,
        active_subscriptions,
        verification_state,
        grover_cash_balance,
        last_active_subscription_product_name,
        last_active_subscription_category,
        last_active_subscription_subcategory,
        total_subscriptions,
        ad_user_data,
        ad_personalization,
        lifetime_rented_variant_sku,
        date_first_active_subscription,
        first_ever_rented_variant_sku,
        potential_churner,
        ROW_NUMBER () OVER (PARTITION BY external_id ORDER BY extracted_at DESC ) AS row_num
FROM :braze_hist_table;)
SELECT
        external_id,
        "_update_existing_only",
        customer_label,
        profile_status,
        company_status,
        customer_type,
        company_created_at,
        trust_type,
        company_type,
        qualified_for_review_invitation,
        lifetime_rented_product,
        lifetime_rented_category,
        lifetime_rented_subcategory,
        lifetime_rented_brand,
        lifetime_rented_sku,
        lifetime_rental_plan,
        lifetime_voucher_redeemed,
        rfm_segment,
        active_subscriptions,
        verification_state,
        grover_cash_balance,
        last_active_subscription_product_name,
        last_active_subscription_category,
        last_active_subscription_subcategory,
        total_subscriptions,
        ad_user_data,
        ad_personalization,
        lifetime_rented_variant_sku,
        date_first_active_subscription,
        first_ever_rented_variant_sku,
        potential_churner
FROM :braze_stage_table;
EXCEPT
SELECT
        external_id,
        "_update_existing_only",
        customer_label,
        profile_status,
        company_status,
        customer_type,
        company_created_at,
        trust_type,
        company_type,
        qualified_for_review_invitation,
        lifetime_rented_product,
        lifetime_rented_category,
        lifetime_rented_subcategory,
        lifetime_rented_brand,
        lifetime_rented_sku,
        lifetime_rental_plan,
        lifetime_voucher_redeemed,
        rfm_segment,
        active_subscriptions,
        verification_state,
        grover_cash_balance,
        last_active_subscription_product_name,
        last_active_subscription_category,
        last_active_subscription_subcategory,
        total_subscriptions,
        ad_user_data,
        ad_personalization,
        lifetime_rented_variant_sku,
        date_first_active_subscription,
        first_ever_rented_variant_sku,
        potential_churner
FROM deduped_history
WHERE row_num = 1;
