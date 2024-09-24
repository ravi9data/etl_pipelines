DROP VIEW IF EXISTS dwh.v_daily_kpi_report;
CREATE VIEW dwh.v_daily_kpi_report AS
SELECT
   met.fact_date
  ,met.country_name
  ,met.region
  ,met.store_commercial_split
  ,met.new_recurring
  ,met.category_name
  ,met.segment
  ,met.cancellation_reason_churn
  ,met.active_subscription_value
  ,met.acquired_subscription_value
  ,met.acquired_subscriptions
  ,met.cancelled_subscription_value
  ,met.new_subscriptions_rented_again
  ,met.new_subscription_value_rented_again
  ,met.not_rented_again_subscriptions
  ,met.not_rented_again_subscriptions_value
  ,met.rented_again_subscriptions
  ,met.rented_again_subscriptions_value
  ,tar.active_subs_value_target
  ,tar.incremental_subs_value_target
  ,tar.acquired_subs_value_target
  ,tar.cancelled_subs_value_target
FROM dwh.daily_kpi_subscription_value_metrics_combined met
FULL OUTER JOIN dwh.daily_kpi_subscription_value_targets tar
      ON tar.datum = met.fact_date
      AND tar.store_commercial_split = met.store_commercial_split
      AND tar.country_name = met.country_name
      AND tar.region = met.region
      AND tar.category_name = met.category_name
      AND tar.cancellation_reason_churn = met.cancellation_reason_churn
      AND tar.new_recurring = met.new_recurring
      AND tar.segment = met.segment
WHERE met.country_name != 'United Kingdom'
WITH NO SCHEMA BINDING;
GRANT SELECT ON TABLE dwh.v_daily_kpi_report TO matillion;
