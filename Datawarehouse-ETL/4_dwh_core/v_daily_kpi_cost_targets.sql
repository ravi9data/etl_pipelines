DROP VIEW IF EXISTS dwh.v_daily_kpi_cost_targets;
CREATE VIEW dwh.v_daily_kpi_cost_targets AS
WITH 
cost_targets AS (
  SELECT country,
         month,
         DATE_DIFF('day', month, (DATE_ADD('month', 1, month))) as total_days,
         marketing_cost_target / total_days::DECIMAL(10, 2)     AS daily_marketing_cost_target,
         performance_cost_target / total_days::DECIMAL(10, 2)   AS daily_performance_cost_target,
         brand_cost_target / total_days::DECIMAL(10, 2)         AS daily_brand_cost_target,
         cps_target,
         cps_performance_target
  FROM marketing.monthly_cost_targets),


actual_costs_pre AS (
  SELECT reporting_date,
         country,
         brand_non_brand,
         cash_non_cash,
         CASE WHEN brand_non_brand = 'Brand' AND cash_non_cash = 'Non-Cash' THEN sum(total_spent_eur) END AS brand_non_cash,
         CASE WHEN brand_non_brand = 'Brand' AND cash_non_cash = 'Cash' THEN sum(total_spent_eur) END     AS brand_cash,
         CASE WHEN brand_non_brand = 'Performance' AND cash_non_cash = 'Non-Cash' THEN sum(total_spent_eur) END AS performance_non_cash,
         CASE WHEN brand_non_brand = 'Performance' AND cash_non_cash = 'Cash' THEN sum(total_spent_eur) END     AS performance_cash
  FROM marketing.marketing_cost_daily_combined
  GROUP BY 1, 2, 3, 4),

actual_costs AS (
  SELECT reporting_date,
         country,
         COALESCE(SUM(brand_non_cash),0)             AS brand_non_cash_cost,
         COALESCE(SUM(brand_cash),0)                 AS brand_cash_cost,
         COALESCE(SUM(performance_non_cash),0)       AS performance_non_cash_cost,
         COALESCE(SUM(performance_cash),0)           AS performance_cash_cost,
         performance_non_cash_cost + brand_non_cash_cost + brand_cash_cost + performance_cash_cost AS total_marketing_cost
  FROM actual_costs_pre
  GROUP BY 1, 2)

  SELECT reporting_date,
         ac.country,
         brand_non_cash_cost,
         brand_cash_cost,
         performance_non_cash_cost,
         performance_cash_cost,
         total_marketing_cost,
         daily_marketing_cost_target,
         daily_performance_cost_target,
         daily_brand_cost_target,
         cps_target,
         cps_performance_target
  FROM actual_costs ac
    LEFT JOIN cost_targets ct
      ON ac.country = ct.country
      AND date_trunc('month', reporting_date) = ct.month
  ORDER BY 2, 1
  WITH NO SCHEMA BINDING;
  GRANT SELECT ON TABLE dwh.v_daily_kpi_cost_targets TO matillion;