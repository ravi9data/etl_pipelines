DROP TABLE IF EXISTS ods_spv_historical_test.cashflow_due;
CREATE TABLE ods_spv_historical_test.cashflow_due AS
SELECT DISTINCT
   pa.asset_id,
   DATE_TRUNC('MONTH', due_date)::DATE AS due_date,
   SUM(
   CASE
      WHEN
         payment_type = 'REPAIR COST'
      THEN
         amount_due
      ELSE
         0
   END
) AS repair_cost_due , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'FIRST', 'RECURRENT'
         )
      THEN
         amount_due
      ELSE
         0
   END
) AS subscription_revenue_due , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'SHIPMENT'
         )
      THEN
         amount_due
      ELSE
         0
   END
) AS shipment_cost_due , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'CUSTOMER BOUGHT'
         )
      THEN
         amount_due
      ELSE
         0
   END
) AS customer_bought_due , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'GROVER SOLD'
         )
      THEN
         amount_due
      ELSE
         0
   END
   CASE
      WHEN
         payment_type IN
         (
            'DEBT COLLECTION'
         )
      THEN
         amount_due
      ELSE
         0
   END
) AS debt_collection_due , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'ADDITIONAL CHARGE', 'COMPENSATION'
         )
      THEN
         amount_due
      ELSE
         0
   END
) AS additional_charge_due , SUM(
   CASE
      WHEN
         payment_type = 'CHARGE BACK SUBSCRIPTION_PAYMENT'
      THEN
         amount_due
      ELSE
         0
   END
) AS sub_payment_chargeback_due , SUM(
   CASE
      WHEN
         payment_type = 'CHARGE BACK ASSET_PAYMENT'
      THEN
         amount_due
      ELSE
         0
   END
) AS asset_payment_chargeback_due , SUM(
   CASE
      WHEN
         payment_type = 'REFUND SUBSCRIPTION_PAYMENT'
      THEN
         amount_due
      ELSE
         0
   END
) AS refund_sub_payment_due , SUM(
   CASE
      WHEN
         payment_type = 'REFUND ASSET_PAYMENT'
      THEN
         amount_due
      ELSE
         0
   END
) AS refund_asset_payment_due , SUM(
   CASE
      WHEN
         payment_type = 'REFUND UNKNOWN'
      THEN
         amount_due
      ELSE
         0
   END
) AS refund_other_payment_due
FROM
   ods_production.payment_all pa
   LEFT JOIN
      ods_production.asset a
      ON pa.asset_id = a.asset_id
GROUP BY
   1, 2;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.cashflow;
CREATE TABLE ods_spv_historical_test.cashflow AS
SELECT DISTINCT
   pa.asset_id,
   GREATEST(DATE_TRUNC('MONTH', paid_date), DATE_TRUNC('MONTH', a.purchased_date))::DATE AS datum_month,
   SUM(
   CASE
      WHEN
         payment_type = 'REPAIR COST'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS repair_cost_paid , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'FIRST', 'RECURRENT'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS subscription_revenue_paid , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'SHIPMENT'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS shipment_cost_paid , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'CUSTOMER BOUGHT'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS customer_bought_paid , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'GROVER SOLD'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
   CASE
      WHEN
         payment_type IN
         (
            'DEBT COLLECTION'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS debt_collection_paid , SUM(
   CASE
      WHEN
         payment_type IN
         (
            'ADDITIONAL CHARGE', 'COMPENSATION'
         )
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS additional_charge_paid , SUM(
   CASE
      WHEN
         payment_type = 'CHARGE BACK SUBSCRIPTION_PAYMENT'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS sub_payment_chargeback_paid , SUM(
   CASE
      WHEN
         payment_type = 'CHARGE BACK ASSET_PAYMENT'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS asset_payment_chargeback_paid , SUM(
   CASE
      WHEN
         payment_type = 'REFUND SUBSCRIPTION_PAYMENT'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS refund_sub_payment_paid , SUM(
   CASE
      WHEN
         payment_type = 'REFUND ASSET_PAYMENT'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS refund_asset_payment_paid , SUM(
   CASE
      WHEN
         payment_type = 'REFUND UNKNOWN'
         AND status = 'PAID'
      THEN
         amount_paid
      ELSE
         0
   END
) AS refund_other_payment_paid
FROM
   ods_production.payment_all pa
   LEFT JOIN
      ods_production.asset a
      ON pa.asset_id = a.asset_id
GROUP BY
   1, 2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.paid_reporting_month;
CREATE TABLE ods_spv_historical_test.paid_reporting_month AS
SELECT DISTINCT
   spv.asset_id,
   spv.reporting_date::DATE,
   SUM(COALESCE(subscription_revenue_paid, 0)) AS subscription_revenue_paid_reporting_month,
   SUM(COALESCE(repair_cost_paid, 0)) AS other_charges_paid_reporting_month,
   SUM(COALESCE(debt_collection_paid, 0)) AS debt_collection_paid_reporting_month,
   SUM(COALESCE(additional_charge_paid, 0) + COALESCE(shipment_cost_paid, 0)) AS additional_charges_paid_reporting_month,
   SUM(COALESCE(refund_sub_payment_paid, 0) + COALESCE(sub_payment_chargeback_paid, 0)) AS refunds_chb_subscription_paid_reporting_month,
   SUM(COALESCE(refund_asset_payment_paid, 0) + COALESCE(asset_payment_chargeback_paid, 0)) AS refunds_chb_asset_paid_reporting_month,
   SUM(COALESCE(refund_other_payment_paid, 0)) AS refunds_chb_others_paid_reporting_month
FROM
   ods_spv_historical_test.cashflow acc
   INNER JOIN
      ods_spv_historical_test.spv_report_master spv
      ON spv.asset_id = acc.asset_Id
      AND DATE_TRUNC('MONTH', spv.reporting_date)::DATE = DATE_TRUNC('MONTH', acc.datum_month)::DATE
GROUP BY
   1,
   2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.due_reporting_month;
CREATE TABLE ods_spv_historical_test.due_reporting_month AS
SELECT DISTINCT
   spv.asset_id,
   spv.reporting_date::DATE,
   --subscription revenue net due
   SUM(COALESCE(subscription_revenue_due, 0)) + SUM(COALESCE(refund_sub_payment_due, 0)) + SUM(COALESCE(sub_payment_chargeback_due, 0)) AS subscription_revenue_due_reporting_month,
   --other revenue net due
   --total revenue net due
FROM
   ods_spv_historical_test.cashflow_due acc
   INNER JOIN
      ods_spv_historical_test.spv_report_master spv
      ON spv.asset_id = acc.asset_Id
      AND DATE_TRUNC('MONTH', spv.reporting_date)::DATE = DATE_TRUNC('MONTH', acc.due_date)::DATE
GROUP BY
   1,
   2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.lifetime;
CREATE TABLE ods_spv_historical_test.lifetime AS
SELECT DISTINCT
   spv.asset_id,
   spv.reporting_date::DATE,
   SUM(COALESCE(subscription_revenue_paid, 0)) AS subscription_revenue_paid_lifetime,
   SUM(COALESCE(repair_cost_paid, 0) + COALESCE(shipment_cost_paid, 0)) AS other_charges_paid_lifetime,
   SUM(COALESCE(debt_collection_paid, 0)) AS debt_collection_paid_lifetime,
   SUM(COALESCE(additional_charge_paid, 0)) AS additional_charges_paid_lifetime,
   SUM(COALESCE(refund_sub_payment_paid, 0) + COALESCE(sub_payment_chargeback_paid, 0)) AS refunds_chb_subscription_paid_lifetime,
   SUM(COALESCE(refund_asset_payment_paid, 0) + COALESCE(asset_payment_chargeback_paid, 0)) AS refunds_chb_asset_paid_lifetime,
   SUM(COALESCE(refund_other_payment_paid, 0)) AS refunds_chb_others_paid_lifetime
FROM
   ods_spv_historical_test.cashflow acc
   LEFT JOIN
      ods_spv_historical_test.spv_report_master spv
      ON spv.asset_id = acc.asset_Id
      AND DATE_TRUNC('MONTH', spv.reporting_date)::DATE >= DATE_TRUNC('MONTH', acc.datum_month)::DATE
GROUP BY
   1,
   2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.due_lifetime;
CREATE TABLE ods_spv_historical_test.due_lifetime AS
SELECT DISTINCT
   spv.asset_id,
   spv.reporting_date::DATE,
   --subscription revenue net due
   SUM(COALESCE(subscription_revenue_due, 0)) + SUM(COALESCE(refund_sub_payment_due, 0)) + SUM(COALESCE(sub_payment_chargeback_due, 0)) AS subscription_revenue_due_lifetime,
   --other revenue net due
   --total revenue net due
FROM
   ods_spv_historical_test.cashflow_due acc
   INNER JOIN
      ods_spv_historical_test.spv_report_master spv
      ON spv.asset_id = acc.asset_Id
      AND DATE_TRUNC('MONTH', spv.reporting_date)::DATE >= DATE_TRUNC('MONTH', acc.due_date)::DATE
GROUP BY
   1,
   2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.subs_per_asset;
CREATE TABLE ods_spv_historical_test.subs_per_asset AS
SELECT
   ah.asset_id,
   COUNT(DISTINCT sh.subscription_id) AS total_subscriptions_per_asset
FROM
   master.allocation_historical ah
   LEFT JOIN
      master.subscription_historical sh
      ON ah.subscription_id = sh.subscription_id
      AND ah.date = sh.date
WHERE
   ah.date <= '2023-07-31'
GROUP BY
   1 ;
----------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.sub_recon;
CREATE TABLE ods_spv_historical_test.sub_recon AS
SELECT
   LAST_DAY(DATE_TRUNC('MONTH', s."date"))::DATE AS reporing_month,
   a.asset_id,
   COUNT(DISTINCT
   CASE
      WHEN
         status = 'ACTIVE'
         AND s."date" = DATE_TRUNC('MONTH', s."date")
      THEN
         s.subscription_id
   END
) AS active_subscriptions_bom, COUNT(DISTINCT
   CASE
      WHEN
         status = 'ACTIVE'
         AND s."date" = LAST_DAY(DATE_TRUNC('MONTH', s."date"))
      THEN
         s.subscription_id
   END
) AS active_subscriptions_eom, COUNT(DISTINCT
   CASE
      WHEN
         status = 'ACTIVE'
      THEN
         s.subscription_id
   END
) AS active_subscriptions, COUNT(DISTINCT
   CASE
      WHEN
         status = 'ACTIVE'
         AND start_date::DATE = s.date::DATE
      THEN
         s.subscription_id
   END
) AS acquired_subscriptions, COUNT(DISTINCT
   CASE
      WHEN
         status = 'ACTIVE'
         AND start_date::DATE < DATE_TRUNC('MONTH', s."date")
      THEN
         s.subscription_id
   END
) AS rollover_subscriptions, COUNT(DISTINCT
   CASE
      WHEN
         cancellation_date::DATE = s."date"
      THEN
         s.subscription_id
   END
) AS cancelled_subscriptions
FROM
   master.subscription_historical s
   LEFT JOIN
      master.allocation_historical a
      ON s.subscription_id = a.subscription_id
      AND s."date" = a."date"
WHERE
   TRUE
   AND DATE_TRUNC('MONTH', s.date) = '2023-07-01'
   AND a.allocation_status_original NOT IN
   (
      'CANCELLED'
   )
GROUP BY
   1, 2 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.active_value;
CREATE TABLE ods_spv_historical_test.active_value AS
SELECT DISTINCT
   LAST_DAY(s.date)::DATE AS reporting_month,
   s.subscription_id,
   a.asset_id,
   s.subscription_value,
   MAX(
   CASE
      WHEN
         s.start_date::DATE = s.date::DATE
      THEN
         1
      ELSE
         0
   END
) AS is_acquired
FROM
   master.subscription_historical s
   LEFT JOIN
      master.allocation_historical a
      ON s.subscription_id = a.subscription_id
      AND s."date" = a."date"
WHERE
   TRUE
   AND DATE_TRUNC('MONTH', s.date)::DATE = '2023-07-01'
   AND a.allocation_status_original NOT IN
   (
      'CANCELLED'
   )
   AND s.status = 'ACTIVE'
GROUP BY
   1, 2, 3, 4 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.cancelled_value;
CREATE TABLE ods_spv_historical_test.cancelled_value AS
SELECT DISTINCT
   LAST_DAY(DATE_TRUNC('MONTH', s."date"))::DATE AS reporting_month,
   s.subscription_id,
   a.asset_id,
   s.subscription_value
FROM
   master.subscription_historical s
   LEFT JOIN
      master.allocation_historical a
      ON s.subscription_id = a.subscription_id
      AND s."date" = a."date"
WHERE
   TRUE
   AND DATE_TRUNC('MONTH', s.date)::DATE = '2023-07-01'
   AND a.allocation_status_original NOT IN
   (
      'CANCELLED'
   )
   AND s.date::DATE = s.cancellation_date::DATE ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.asset_historical;
CREATE TABLE ods_spv_historical_test.asset_historical AS
SELECT
   *
FROM
   master.asset_historical
WHERE
   date = '2023-07-31' ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.subscription_historical;
CREATE TABLE ods_spv_historical_test.subscription_historical AS
SELECT
   *
FROM
   master.subscription_historical
WHERE
   date = '2023-07-31' ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.order_historical;
CREATE TABLE ods_spv_historical_test.order_historical AS
SELECT
   *
FROM
   master.order_historical
WHERE
   date = '2023-07-31' ;
-------------------------------------------------------------------------------------------------------------------------------------
/*DROP TABLE IF EXISTS ods_spv_historical_test.spv_report_master_202307;
CREATE TABLE ods_spv_historical_test.spv_report_master_202307 AS
SELECT
   *
FROM
   ods_spv_historical_test.spv_report_master_202307 ;*/ --Not neccessary
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.supplier;
CREATE TABLE ods_spv_historical_test.supplier AS
SELECT
   *
FROM
   ods_production.supplier ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.asset_category;
CREATE TABLE ods_spv_historical_test.asset_category AS WITH asset_category_base AS
(
   SELECT
      asset_id,
      category_name AS category,
      subcategory_name AS subcategory,
      brand,
      RANK() OVER (PARTITION BY asset_id
   ORDER BY
      date DESC) AS cat_rank
   FROM
      master.asset_historical ah
   WHERE
      category_name IS NOT NULL
      AND ah.date <= '2023-07-31'
)
SELECT
   *
FROM
   asset_category_base
WHERE
   cat_rank = 1 ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.cust;
CREATE TABLE ods_spv_historical_test.cust AS
SELECT
   ch.customer_id,
   ch.customer_type,
   ch.burgel_risk_category,
   ch.shipping_city,
   ch.shipping_country,
   ch.shipping_zip,
   ch.first_subscription_acquisition_channel,
   ch.customer_acquisition_cohort,
   ch.active_subscriptions,
   COALESCE(ch.subscription_revenue_paid, 0) - (COALESCE(ch.subscription_revenue_chargeback, 0) + COALESCE(ch.subscription_revenue_refunded, 0) ) AS net_subscription_revenue_paid
FROM
   master.customer_historical ch
WHERE
   ch.date = '2023-07-31' ;
-------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS ods_spv_historical_test.customer_score;
CREATE TABLE ods_spv_historical_test.customer_score AS WITH spain_equifax AS
(
   SELECT DISTINCT
      customer_id::INT,
      'es_equifax' AS score_provider,
      LAST_VALUE(score_value::DECIMAL(38, 0)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS es_equifax_score_value
   FROM
      s3_spectrum_rds_dwh_order_approval.equifax_risk_score_data s
   WHERE
      score_value IS NOT null
      AND updated_at <= '2023-07-31'
)
,
germany_schufa AS
(
   SELECT DISTINCT
      customer_id::INT,
      'de_schufa' AS score_provider,
      LAST_VALUE(score_range::TEXT) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS de_schufa_score_rating
   FROM
      s3_spectrum_rds_dwh_order_approval.schufa_data s
   WHERE
      score_range IS NOT null
      AND updated_at <= '2023-07-31'
)
,
netherlands_focum AS
(
   SELECT DISTINCT
      customer_id::INT,
      'nl_focum' AS score_provider,
      LAST_VALUE(risk_score::DECIMAL(38, 0)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nl_focum_score_value
   FROM
      s3_spectrum_rds_dwh_order_approval.focum_data s
   WHERE
      risk_score IS NOT null
      AND updated_at <= '2023-07-31'
),
crif AS
(
   SELECT DISTINCT
      customer_id::INT,
      'crif' AS score_provider,
      LAST_VALUE(score_value::DECIMAL(38, 2)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS crif_score_value
   FROM
      s3_spectrum_rds_dwh_order_approval.crifburgel_data s
   WHERE
	  updated_at <= '2023-07-31'
),
burgel AS
(
   SELECT DISTINCT
      customer_id::INT,
      'burgel' AS score_provider,
      LAST_VALUE(burgel_score::DECIMAL(38, 1)) OVER (PARTITION BY customer_id
   ORDER BY
      creation_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS burgel_score
   FROM
      finance.credit_score_burgel_2022_11_15
   WHERE
	  creation_timestamp <= '2023-07-31'
),
kvm AS
(
   SELECT DISTINCT
      customer_id::INT,
      'kvm_score' AS score_provider,
      LAST_VALUE(ksv_score::DECIMAL(38, 0)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS kvm_score
   FROM
      finance.credit_score_kvm_2022_11_15
   WHERE
	  updated_at <= '2023-07-31'
),
nl_experian AS
(
   SELECT DISTINCT
      customer_id::INT,
      'nl_experian' AS score_provider,
      LAST_VALUE(score_value::DECIMAL(38, 2)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nl_experian_score
   FROM
      s3_spectrum_rds_dwh_order_approval.experian_data s
   WHERE
	  updated_at <= '2023-07-31'
),
es_delphi AS
(
   SELECT DISTINCT
      customer_id::INT,
      'es_experian' AS score_provider,
      LAST_VALUE(score_value::DECIMAL(38, 2)) OVER (PARTITION BY customer_id
   ORDER BY
      updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS es_delphi_score
   FROM
      s3_spectrum_rds_dwh_order_approval.experian_es_delphi_data s
   WHERE
	  updated_at <= '2023-07-31'
)
SELECT DISTINCT
   cust.customer_id,
   sch.de_schufa_score_rating,
   crif.crif_score_value,
   burgel.burgel_score,
   kvm.kvm_score,
   fo.nl_focum_score_value,
   ne.nl_experian_score,
   sp.es_equifax_score_value,
   ee.es_delphi_score
FROM
   ods_spv_historical_test.cust cust
   LEFT JOIN
      spain_equifax sp
      ON cust.customer_id = sp.customer_id
   LEFT JOIN
      germany_schufa sch
      ON cust.customer_id = sch.customer_id
   LEFT JOIN
      netherlands_focum fo
      ON cust.customer_id = fo.customer_id
   LEFT JOIN
      crif
      ON cust.customer_id = crif.customer_id
   LEFT JOIN
      burgel
      ON cust.customer_id = burgel.customer_id
   LEFT JOIN
      nl_experian ne
      ON cust.customer_id = ne.customer_id
   LEFT JOIN
      es_delphi ee
      ON cust.customer_id = ee.customer_id
   LEFT JOIN
      kvm
      ON cust.customer_id = kvm.customer_id ;
-------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.asset_cohort_month;
CREATE TABLE ods_spv_historical_test.asset_cohort_month AS
SELECT DISTINCT
   asset_id,
   shifted_creation_cohort AS shifted_creation_cohort
FROM
   dm_finance.asset_collection_curves_historical
WHERE
   reporting_date = '2023-07-31'
;
-------------------------------------LUXCO REPORTING----------------------------------------------------------------------------------------------
-------------------------------------LUXCO REPORTING----------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS ods_spv_historical_test.luxco_reporting_202307;
CREATE TABLE ods_spv_historical_test.luxco_reporting_202307 AS
(
   WITH active_value_final AS
   (
      SELECT
         asset_id,
         reporting_month,
         SUM(subscription_value) AS active_subscription_value,
         SUM(
         CASE
            WHEN
               is_acquired = 1
            THEN
               subscription_value
         END
) AS acquired_subscription_value, SUM(
         CASE
            WHEN
               is_acquired = 0
            THEN
               subscription_value
         END
) AS rollover_subscription_value
      FROM
         ods_spv_historical_test.active_value
      GROUP BY
         1, 2
   )
, cancelled_value_final AS
   (
      SELECT
         asset_id,
         reporting_month,
         SUM(subscription_value) AS cancelled_subscription_value
      FROM
         ods_spv_historical_test.cancelled_value
      GROUP BY
         1,
         2
   )
,
   supplier AS
   (
      SELECT
         agan_asset_supplier,
         supplier_name
      FROM
         ods_spv_historical_test.supplier
   )
,
   prev_val AS
   (
      SELECT
         asset_id,
         original_valuation AS previous_original_valuation,
         final_valuation AS previous_final_valuation
      FROM
         dm_finance.luxco_report_master srm
      WHERE
         reporting_date = DATE('2023-07-01') - 1
   )
,
   allocation_pre AS
   (
      SELECT
         ah.asset_id,
         ah.allocation_id,
         a.order_id,
         ah.allocation_status_original,
         COALESCE(sh.subscription_id, ah.subscription_id) AS subscription_id,
         sh.start_date,
         sh.status,
         ah2.sold_date,
         ah2.sold_price,
         ROW_NUMBER() OVER (PARTITION BY ah.asset_id
      ORDER BY
         ah.allocated_at DESC) idx
      FROM
         master.allocation_historical ah
         LEFT JOIN
            ods_production.allocation a
            ON a.allocation_id = ah.allocation_id
         LEFT JOIN
            master.subscription_historical sh
            ON ah.subscription_id = sh.subscription_id
            AND ah.date = sh.date
         LEFT JOIN
            master.asset_historical ah2
            ON ah2.asset_id = ah.asset_id
            AND ah2.date = ah.date
      WHERE
         ah.date = '2023-07-31'
   )
,
   allocation AS
   (
      SELECT
         *
      FROM
         allocation_pre
      WHERE
         idx = 1
   )
   SELECT DISTINCT
      spv.reporting_date,
      spv.asset_id,
      spv.serial_number,
      acat.brand,
      REPLACE(REGEXP_REPLACE(TRANSLATE(spv.asset_name, 'üöéßóàáúóäíñèâçò’ë', 'uoesoaauoaine acoe')
 , '[^[:alnum:][:blank:][:punct:]]', ''), ' ', ' ') AS asset_name, spv.product_sku, spv.warehouse, spv.capital_source_name, acat.category, acat.subcategory, spv.purchased_date, spv.initial_price AS purchase_price,
      CASE
         WHEN
            ss.agan_asset_supplier = 'true'
         THEN
            'AGAN'
         ELSE
            'NEW'
      END
      AS condition_on_purchase, spv.asset_condition_spv AS condition,
      CASE
         WHEN
            DATE_TRUNC('MONTH', h.created_at::DATE) = DATE_TRUNC('MONTH', spv.reporting_date::DATE)
         THEN
            1
         ELSE
            0
      END
      AS asset_added_to_portfolio,
      CASE
         WHEN
            DATE_TRUNC('MONTH', date_transfer_assets::DATE) = DATE_TRUNC('MONTH', spv.reporting_date::DATE)
         THEN
            1
         ELSE
            0
      END
      AS asset_added_to_portfolio_m_and_g, spv.last_allocation_days_in_stock AS days_in_stock, h.last_allocation_dpd,
      CASE
         WHEN
            spv.asset_status_original IN
            (
               'IRREPARABLE', 'RECOVERED'
            )
         THEN
            'IN STOCK'
         WHEN
            spv.asset_status_original LIKE 'WRITTEN OFF%'
         THEN
            'WRITTEN OFF'
         WHEN
            h.last_allocation_dpd BETWEEN 151 AND 180
         THEN
            'OD 151-180'
         WHEN
            h.last_allocation_dpd BETWEEN 181 AND 270
         THEN
            'OD 181-270'
         WHEN
            h.last_allocation_dpd BETWEEN 271 AND 360
         THEN
            'OD 271-360'
         WHEN
            h.last_allocation_dpd > 360
         THEN
            'OD 360+'
         ELSE
            h.dpd_bucket
      END
      AS dpd_bucket_r,
      CASE
         WHEN
            dpd_bucket_r LIKE '%IN STOCK%'
         THEN
            'In Stock'
         WHEN
            dpd_bucket_r = 'LOST'
         THEN
            'Lost'
         WHEN
            dpd_bucket_r = 'SOLD'
         THEN
            'Sold'
         WHEN
            dpd_bucket_r IN
            (
               'PERFORMING', 'OD 1-30'
            )
         THEN
            'Performing'
         WHEN
            dpd_bucket_r = 'WRITTEN OFF'
         THEN
            'Written Off'
         WHEN
            dpd_bucket_r IN
            (
               'PENDING WRITEOFF', 'OD 31-60', 'OD 61-90', 'OD 91-120', 'OD 121-150', 'OD 151-180', 'OD 151-180', 'OD 181-270', 'OD 271-360', 'OD 360+'
            )
         THEN
            'In Debt Collection'
         ELSE
            dpd_bucket_r
      END
      AS asset_position,
      CASE
         WHEN
            spv.asset_status_original IN
            (
               'WRITTEN OFF OPS', 'WRITTEN OFF DC', 'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            'REMOVED'
         WHEN
            asset_added_to_portfolio = 1
         THEN
            'NEW'
         ELSE
            'EXISTING'
      END
      AS asset_classification,
      CASE
         WHEN
            spv.asset_status_original IN
            (
               'WRITTEN OFF OPS', 'WRITTEN OFF DC', 'SOLD', 'LOST', 'LOST SOLVED'
            )
         THEN
            'REMOVED'
         WHEN
            asset_added_to_portfolio_m_and_g = 1
         THEN
            'NEW'
         ELSE
            'EXISTING'
      END
      AS asset_classification_m_and_g, spv.asset_status_original, ah.allocation_status_original, m_since_last_valuation_price, valuation_method, spv.currency, h.asset_value_linear_depr AS discounted_purchase_price, spv.final_price AS original_valuation,
      CASE
         WHEN
            spv.capital_source_name = 'Grover Finance I GmbH'
         THEN
            CASE
               WHEN
                  last_allocation_dpd < 361
               THEN
                  0
               WHEN
                  last_allocation_dpd >= 361
               THEN
                  1
               ELSE
                  0
            END
            WHEN
               spv.capital_source_name = 'USA_test'
            THEN
               CASE
                  WHEN
                     last_allocation_dpd BETWEEN 181 AND 270
                  THEN
                     0.25
                  WHEN
                     last_allocation_dpd BETWEEN 271 AND 360
                  THEN
                     0.5
                  WHEN
                     last_allocation_dpd >= 361
                  THEN
                     1
                  ELSE
                     0
               END
            WHEN
               spv.capital_source_name not in ('Grover Finance I GmbH','USA_test')
            THEN
               CASE
                  WHEN
                     last_allocation_dpd BETWEEN 91 AND 180
                  THEN
                     0.25
                  WHEN
                     last_allocation_dpd BETWEEN 181 AND 270
                  THEN
                     0.5
                  WHEN
                     last_allocation_dpd BETWEEN 271 AND 360
                  THEN
                     0.75
                  WHEN
                     last_allocation_dpd >= 361
                  THEN
                     1
                  ELSE
                     0
               END
      END
      AS impairment_rate,
      (
         1 - impairment_rate
      )
      * original_valuation AS final_valuation, prev.previous_original_valuation, prev.previous_final_valuation,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            (s.committed_sub_value + COALESCE(s.additional_committed_sub_value,0))
      END
      AS committed_subscription_value,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            s.commited_sub_revenue_future
      END
      AS commited_sub_revenue_future,
      CASE
         WHEN
            prev.previous_original_valuation = 0
            AND original_valuation > 0
         THEN
            0
         WHEN
            prev.previous_original_valuation = 0
            AND original_valuation = 0
         THEN
            0
         ELSE
(original_valuation / prev.previous_original_valuation) - 1
      END
      AS depreciation,
      CASE
         WHEN
            depreciation * 100 < - 99
         THEN
            '-100%'
         WHEN
            depreciation * 100 >= - 99
            AND depreciation * 100 <= - 50
         THEN
            '-99% to -50%'
         WHEN
            depreciation * 100 > - 50
            AND depreciation * 100 <= - 25
         THEN
            '-49% to -25%'
         WHEN
            depreciation * 100 > - 25
            AND depreciation * 100 <= - 10
         THEN
            '-24% to -10%'
         WHEN
            depreciation * 100 > - 10
            AND depreciation * 100 <= - 5
         THEN
            '-9% to -5%'
         WHEN
            depreciation * 100 > - 5
            AND depreciation * 100 <= 4
         THEN
            '-4% to 4%'
         WHEN
            depreciation * 100 > 4
            AND depreciation * 100 <= 15
         THEN
            '5% to 15%'
         WHEN
            depreciation * 100 > 15
         THEN
            '+15%'
         ELSE
            'NA'
      END
      AS depreciation_buckets, 		--Current Subscription Data
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            NULLIF(ah.subscription_id, '')
      END
      AS subscription_id, s.status,
      CASE
         WHEN
            s.status = 'ACTIVE'
            AND subscription_plan IN
            (
               '1 Months', '2 Months', 'Pay AS You Go'
            )
         THEN
            '1 month'
         WHEN
            s.status = 'ACTIVE'
            AND subscription_plan NOT IN
            (
               '1 Months', '2 Months', 'Pay AS You Go'
            )
         THEN
            subscription_plan
         ELSE
            NULL
      END
      AS subscription_plan,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            s.start_date::DATE
      END
      AS start_date,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            DATE_PART('year', s.date)
      END
      AS year_r,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            EXTRACT('Month'
   FROM
      s.date) + 1
      END
      AS month_r,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            DATE_PART('day', s.start_date)
      END
      AS day_r,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            TO_DATE((year_r || '-0' || month_r || '-' || day_r), 'yyyy-mm-dd')
      END
      AS replacement_date,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         WHEN
            s.status = 'ACTIVE'
         THEN
            CASE
               WHEN
                  s.minimum_cancellation_date IS NULL
                  AND s.subscription_plan LIKE 'Pay%'
               THEN
                  replacement_date
               WHEN
                  s.minimum_cancellation_date IS NULL
                  AND s.subscription_plan NOT LIKE 'Pay%'
               THEN
                  DATEADD('MONTH', CAST(s.rental_period AS integer), s.start_date)::DATE
               ELSE
                  s.minimum_cancellation_date
            END
            ELSE
               NULL
      END
      AS maturity_date,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            s.rental_period
      END
      AS committed_duration,
      CASE
         WHEN
            s.status = 'ACTIVE'
         THEN
            DATEDIFF('MONTH', s.start_date, spv.reporting_date)
         ELSE
            NULL
      END
      AS current_duration_calculated,
      CASE
         WHEN
            s.status = 'ACTIVE'
            AND committed_duration >= current_duration_calculated
         THEN
            committed_duration
         WHEN
            s.status = 'ACTIVE'
            AND committed_duration < current_duration_calculated
         THEN
            current_duration_calculated
         ELSE
            NULL
      END
      AS effective_duration,
      CASE
         WHEN
            s.status = 'ACTIVE'
            AND committed_duration <= current_duration_calculated
         THEN
            1
         WHEN
            s.status = 'ACTIVE'
            AND committed_duration > current_duration_calculated
         THEN
            committed_duration - current_duration_calculated
         ELSE
            NULL
      END
      AS outstanding_duration_calcuated,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            avf.active_subscription_value
      END
      AS current_subscription_amount,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            ROUND(h.avg_subscription_amount, 2)
      END
      AS avg_subscription_amount,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            ROUND(h.max_subscription_amount, 2)
      END
      AS max_subscription_amount, spa.total_subscriptions_per_asset, 		-----Customer Data
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            COALESCE(h.customer_id::TEXT, s.customer_id::TEXT, cust.customer_id::TEXT, o.customer_id::TEXT)
      END
      AS customer_id,
      CASE
         WHEN
            s.subscription_id IS NULL
            OR s.status = 'CANCELLED'
         THEN
            NULL
         WHEN
            cust.customer_type::TEXT ILIKE '%normal%'
            OR s.customer_type::TEXT ILIKE '%normal%'
         THEN
            'B2C'
         WHEN
            cust.customer_type::TEXT ILIKE '%business%'
            OR s.customer_type::TEXT ILIKE '%business%'
         THEN
            'B2B'
         ELSE
            COALESCE(cust.customer_type::TEXT, s.customer_type::TEXT, o.customer_type::TEXT)
      END
      AS customer_type,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            COALESCE(cust.shipping_country::TEXT, s.country_name::TEXT, h.country)
      END
      AS country,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            REPLACE(REGEXP_REPLACE(TRANSLATE(COALESCE(cust.shipping_city::TEXT, h.city::TEXT), 'üöéßóàáúóäíñèâçò’ë', 'uoesoaauoaine acoe')
 , '[^[:alnum:][:blank:][:punct:]]', ''), ' ', ' ')
      END
      AS city,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            COALESCE(cust.shipping_zip::TEXT, h.postal_code::TEXT)
      END
      AS postal_code,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            cust.burgel_risk_category
      END
      AS customer_risk_category,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            cust.active_subscriptions
      END
      AS customer_active_subscriptions,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            cust.net_subscription_revenue_paid
      END
      AS customer_net_revenue_paid,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            cust.customer_acquisition_cohort
      END
      AS customer_acquisition_month,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            cust.first_subscription_acquisition_channel
      END
      AS cust_acquisition_channel, 		--Sub Revenue Reporting Month
      --REV Paid
      cfr.subscription_revenue_paid_reporting_month, cfr.asset_sales_revenue_paid_reporting_month + cfr.other_charges_paid_reporting_month + cfr.debt_collection_paid_reporting_month + cfr.additional_charges_paid_reporting_month AS other_revenue_paid_reporting_month, cfr.subscription_revenue_paid_reporting_month + cfr.asset_sales_revenue_paid_reporting_month + cfr.other_charges_paid_reporting_month + cfr.debt_collection_paid_reporting_month + cfr.additional_charges_paid_reporting_month AS total_asset_inflow_reporting_month, 		--Refunds PAID
      cfr.refunds_chb_subscription_paid_reporting_month, cfr.refunds_chb_others_paid_reporting_month + cfr.refunds_chb_asset_paid_reporting_month AS refunds_chb_others_paid_reporting_month_r, cfr.refunds_chb_subscription_paid_reporting_month + cfr.refunds_chb_asset_paid_reporting_month + cfr.refunds_chb_others_paid_reporting_month AS total_refunds_chb_reporting_month, 		--NET INFLOW
      total_asset_inflow_reporting_month + total_refunds_chb_reporting_month AS total_net_asset_inflow_reporting_month, 		--Due NET OFF REFUNDS
      COALESCE(drm.subscription_revenue_due_reporting_month, 0) AS subscription_revenue_due_reporting_month, COALESCE(drm.other_revenue_due_reporting_month, 0) AS other_revenue_due_reporting_month, COALESCE(drm.total_revenue_due_reporting_month, 0) AS total_revenue_due_reporting_month, 		--Overdue
      COALESCE(drm.subscription_revenue_due_reporting_month, 0) - (COALESCE(cfr.subscription_revenue_paid_reporting_month, 0) + COALESCE(cfr.refunds_chb_subscription_paid_reporting_month, 0) ) AS subscription_revenue_overdue_reporting_month, COALESCE(drm.other_revenue_due_reporting_month, 0) - (COALESCE(other_revenue_paid_reporting_month, 0) + COALESCE(refunds_chb_others_paid_reporting_month_r, 0) ) AS other_revenue_overdue_reporting_month, COALESCE(drm.total_revenue_due_reporting_month, 0) - COALESCE(total_net_asset_inflow_reporting_month, 0) AS total_revenue_overdue_reporting_month, 		--Sub Revenue Lifetime
      --Rev Paid
      cfl.subscription_revenue_paid_lifetime, cfl.asset_sales_revenue_paid_lifetime + cfl.other_charges_paid_lifetime + cfl.debt_collection_paid_lifetime + cfl.additional_charges_paid_lifetime AS other_charges_paid_lifetime_r, cfl.subscription_revenue_paid_lifetime + cfl.asset_sales_revenue_paid_lifetime + cfl.other_charges_paid_lifetime + cfl.debt_collection_paid_lifetime + cfl.additional_charges_paid_lifetime AS total_asset_inflow_lifetime, 		--Refunds Paid
      cfl.refunds_chb_subscription_paid_lifetime, COALESCE(cfl.refunds_chb_asset_paid_lifetime, 0) + COALESCE(cfl.refunds_chb_others_paid_lifetime, 0) AS refunds_chb_others_paid_lifetime_r, cfl.refunds_chb_subscription_paid_lifetime + cfl.refunds_chb_asset_paid_lifetime + cfl.refunds_chb_others_paid_lifetime AS total_refunds_chb_lifetime, 		--Net Inflow
      COALESCE(total_asset_inflow_lifetime, 0) + COALESCE(total_refunds_chb_lifetime, 0) AS total_net_inflow_lifetime, 		--Due Net off Refunds
      COALESCE(dl.subscription_revenue_due_lifetime, 0) AS subscription_revenue_due_lifetime, COALESCE(dl.other_revenue_due_lifetime, 0) AS other_revenue_due_lifetime, COALESCE(dl.total_revenue_due_lifetime, 0) AS total_revenue_due_lifetime, 		--Overdue
      COALESCE(dl.subscription_revenue_due_lifetime, 0) - (COALESCE(cfl.subscription_revenue_paid_lifetime, 0) + COALESCE(cfl.refunds_chb_subscription_paid_lifetime, 0) ) AS subscription_revenue_overdue_lifetime, COALESCE(dl.other_revenue_due_lifetime, 0) - (COALESCE(other_charges_paid_lifetime_r, 0) + COALESCE(refunds_chb_others_paid_lifetime_r, 0) ) AS other_revenue_overdue_lifetime, COALESCE(dl.total_revenue_due_lifetime, 0) - COALESCE(total_net_inflow_lifetime, 0) AS overdue_balance_lifetime, 		--asset Sales
      CASE
         WHEN
            spv.asset_status_original LIKE '%SOLD%'
         THEN
            1
         ELSE
            0
      END
      AS is_sold,
      CASE
         WHEN
            prev.previous_original_valuation > 0
            AND original_valuation = 0
            AND DATE_TRUNC('MONTH', a.sold_date::DATE) = DATE_TRUNC('MONTH', spv.reporting_date)
         THEN
            1
         WHEN
            prev.previous_original_valuation > 0
            AND original_valuation = 0
            AND DATE_TRUNC('MONTH', COALESCE(a.sold_date, spv.sold_date, ah.sold_date)::DATE) <> DATE_TRUNC('MONTH', spv.reporting_date)
            AND spv.asset_status_original LIKE '%SOLD%'
         THEN
            1
         WHEN
            spv.asset_status_original LIKE '%SOLD%'
            AND DATE_TRUNC('MONTH', spv.sold_date::DATE) = DATE_TRUNC('MONTH', spv.reporting_date)
         THEN
            1
         ELSE
            0
      END
      AS is_sold_reporting_month,
      CASE
         WHEN
            spv.asset_status_original <> 'SOLD'
         THEN
            NULL
         ELSE
            CASE
               WHEN
                  spv.sold_price = 0
                  AND spv.sold_date IS NOT NULL
                  AND a.sold_price <> 0
               THEN
                  a.sold_price
               ELSE
                  spv.sold_price
            END
      END
      AS sold_price,
      CASE
         WHEN
            prev.previous_original_valuation > 0
            AND original_valuation = 0
            AND DATE_TRUNC('MONTH', a.sold_date::DATE) = DATE_TRUNC('MONTH', spv.reporting_date)
         THEN
            COALESCE(a.sold_date, ah.sold_date)
         WHEN
            prev.previous_original_valuation > 0
            AND original_valuation = 0
            AND DATE_TRUNC('MONTH', a.sold_date::DATE) <> DATE_TRUNC('MONTH', spv.reporting_date)
            AND spv.asset_status_original LIKE '%SOLD%'
         THEN
            COALESCE(a.sold_date, ah.sold_date)
         WHEN
            spv.asset_status_original <> 'SOLD'
         THEN
            NULL
         ELSE
            COALESCE(spv.sold_date, ah.sold_date)
      END
      AS sold_date,
      CASE
         WHEN
            h.asset_status_original = 'SOLD'
         THEN
            h.asset_status_detailed
      END
      AS asset_status_detailed,
      CASE
         WHEN
            spv.asset_status_original = 'SOLD'
            AND spv.sold_date IS NULL
         THEN
            'Date and Payment missing'
         WHEN
            spv.asset_status_original = 'SOLD'
            AND spv.sold_price = 0
         THEN
            'No amount paid'
         WHEN
            spv.asset_status_original = 'SOLD'
            AND spv.sold_date IS NOT NULL
            AND spv.sold_price > 0
         THEN
            'Sold record ok'
         WHEN
            spv.sold_date IS NOT NULL
            AND spv.sold_price > 0
            AND spv.asset_status_original = 'SOLD'
         THEN
            'Not sold status, sold price-date available'
         ELSE
            'Other asset Status'
      END
      AS sold_asset_status_classification, h.last_market_valuation, COALESCE(total_net_inflow_lifetime, 0) - COALESCE(spv.initial_price, 0) AS total_profit,
      CASE
         WHEN
            purchase_price = 0
            OR purchase_price IS NULL
         THEN
            1
         ELSE
        --(total_net_inflow_lifetime + final_valuation)
        (total_net_inflow_lifetime + final_valuation)/purchase_price
      END
      AS return_on_asset, 		--Subscription Value
      r.active_subscriptions, r.active_subscriptions_bom, r.acquired_subscriptions, r.rollover_subscriptions, r.cancelled_subscriptions,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            r.active_subscriptions_eom
      END
      AS active_subscriptions_eom,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            avf.active_subscription_value
      END
      AS active_subscription_value,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            avf.acquired_subscription_value
      END
      AS acquired_subscription_value,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            avf.rollover_subscription_value
      END
      AS rollover_subscription_value, cvf.cancelled_subscription_value,
      CASE
         WHEN
            s.status = 'CANCELLED'
            OR s.status IS NULL
         THEN
            NULL
         ELSE
            COALESCE(avf.active_subscription_value, 0) - COALESCE(cvf.cancelled_subscription_value, 0)
      END
      AS active_subscription_value_eom,
      CASE
	 WHEN 
	    s.buyout_disabled IS TRUE
	 THEN
	    NULL
         WHEN
            s.months_required_to_own ~ '^[0-9]+$'
            AND s.status = 'ACTIVE'
            AND asset_classification <> 'REMOVED'
         THEN
            s.months_required_to_own::INTEGER
         ELSE
            NULL
      END
      AS months_rqd_to_own, TRUNC(ADD_MONTHS(s.start_date, months_rqd_to_own)) AS date_to_own,
      CASE
         WHEN
            date_to_own > s.date
         THEN
            DATEDIFF(MONTH, s.date, date_to_own)
         ELSE
            NULL
      END
      AS months_left_required_to_own,
      CASE
         WHEN
            spv.reporting_date < chm.shifted_creation_cohort
         THEN
            NULL
         ELSE
            chm.shifted_creation_cohort
      END
      AS shifted_creation_cohort,
      CASE
         WHEN
            s.subscription_id IS NULL
            OR s.status = 'CANCELLED'
         THEN
            NULL
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Austria'
         THEN
            cust_sco.crif_score_value::TEXT
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Spain'
         THEN
            COALESCE(cust_sco.es_equifax_score_value, cust_sco.es_delphi_score)::TEXT
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Netherlands'
         THEN
            COALESCE(cust_sco.nl_focum_score_value, cust_sco.nl_experian_score)::TEXT
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Germany'
         THEN
            COALESCE(cust_sco.de_schufa_score_rating, cust_sco.crif_score_value::TEXT, cust_sco.burgel_score::TEXT)::TEXT
      END
      AS customer_score,
      CASE
         WHEN
            s.subscription_id IS NULL
            OR s.status = 'CANCELLED'
         THEN
            NULL::TEXT
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Austria'
            AND cust_sco.crif_score_value IS NOT NULL
         THEN
            'crif'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Spain'
            AND cust_sco.es_equifax_score_value IS NOT NULL
         THEN
            'es_equifax'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Spain'
            AND cust_sco.es_delphi_score IS NOT NULL
         THEN
            'es_delphi'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Netherlands'
            AND cust_sco.nl_focum_score_value IS NOT NULL
         THEN
            'nl_focum'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Netherlands'
            AND cust_sco.nl_experian_score IS NOT NULL
         THEN
            'nl_experian'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.de_schufa_score_rating IS NOT NULL
         THEN
            'de_schufa'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.crif_score_value IS NOT NULL
         THEN
            'crif'
         WHEN
            COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.burgel_score IS NOT NULL
         THEN
            'burgel'
         ELSE
            NULL
      END
      AS credit_buro,
      CASE
         WHEN
            customer_score IS NULL
         THEN
            TRUE
         WHEN
            credit_buro = 'crif'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Austria'
            AND cust_sco.crif_score_value < 420
         THEN
            TRUE
         WHEN
            credit_buro = 'crif'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.crif_score_value > 4
         THEN
            TRUE
         WHEN
            credit_buro = 'de_schufa'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.de_schufa_score_rating IN
            (
               'M', 'N', 'O', 'P'
            )
         THEN
            TRUE
         WHEN
            credit_buro = 'burgel'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Germany'
            AND cust_sco.burgel_score < 3.15
         THEN
            TRUE
         WHEN
            credit_buro = 'nl_focum'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Netherlands'
            AND cust_sco.nl_focum_score_value < 704
         THEN
            TRUE
         WHEN
            credit_buro = 'nl_experian'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Netherlands'
            AND cust_sco.nl_experian_score < 705
         THEN
            TRUE
         WHEN
            credit_buro = 'es_equifax'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Spain'
            AND cust_sco.es_equifax_score_value < 101
         THEN
            TRUE
         WHEN
            credit_buro = 'es_delphi'
            AND COALESCE(s.country_name, cust.shipping_country) = 'Spain'
            AND cust_sco.es_delphi_score < 740
         THEN
            TRUE
         ELSE
            FALSE
      END
      AS is_risky_customer, ROUND((NULLIF(h.market_price_at_purchase_date, 0) - COALESCE(spv.initial_price, 0)) / NULLIF(h.market_price_at_purchase_date, 0), 2) AS asset_purchase_discount_percentage, h.market_price_at_purchase_date,
      CASE
         WHEN
            s.subscription_id IS NOT NULL
            AND s.status <> 'CANCELLED'
         THEN
            s.country_name
      END
      AS rentalco, atr.asset_sale AS asset_transfer, atr.asset_val_or_pp AS asset_value_type, atr.asset_value_purchase_price AS asset_value_transfer, atr.date_transfer_assets::date AS asset_transfer_date,
      (
         1 - impairment_rate
      )
      * original_valuation AS final_valuation_before_impairment_m_n_g,
      CASE 
	WHEN 
		spv.asset_status_original LIKE 'WRITTEN OFF%' 
	THEN 
		spv.final_price_without_written_off 
      	ELSE 
		original_valuation 
      END AS original_valuation_without_written_off   
     FROM
      ods_spv_historical_test.spv_report_master_202307 spv
      LEFT JOIN
         ods_spv_historical_test.asset_historical AS h
         ON spv.asset_id = h.asset_id
      LEFT JOIN
         ods_production.asset AS a
         ON a.asset_id = spv.asset_id
      LEFT JOIN
         allocation AS ah
         ON spv.asset_id = ah.asset_id
      LEFT JOIN
         ods_spv_historical_test.order_historical o
         on ah.order_id = o.order_id
      LEFT JOIN
         ods_spv_historical_test.subscription_historical AS s
         ON ah.subscription_id = s.subscription_id
      LEFT JOIN
         supplier ss
         ON ss.supplier_name = h.supplier
      LEFT JOIN
         ods_spv_historical_test.sub_recon r
         ON r.asset_id = h.asset_id
         AND h.date::DATE = r.reporing_month::DATE
      LEFT JOIN
         ods_spv_historical_test.paid_reporting_month cfr
         ON cfr.asset_id = spv.asset_id
         AND cfr.reporting_date::Date = spv.reporting_date::DATE
      LEFT JOIN
         ods_spv_historical_test.lifetime cfl
         ON cfl.asset_id = spv.asset_id
         AND cfl.reporting_date::DATE = spv.reporting_date::DATE
      LEFT JOIN
         active_value_final avf
         ON avf.asset_id = spv.asset_id
         AND avf.reporting_month::DATE = spv.reporting_date::DATE
      LEFT JOIN
         cancelled_value_final cvf
         ON cvf.asset_id = spv.asset_id
         AND cvf.reporting_month::DATE = spv.reporting_date::DATE
      LEFT JOIN
         ods_spv_historical_test.subs_per_asset spa
         ON spa.asset_id = spv.asset_id
      LEFT JOIN
         ods_spv_historical_test.cust cust
         ON cust.customer_id = COALESCE(h.customer_id::TEXT, s.customer_id::TEXT)
      LEFT JOIN
         ods_spv_historical_test.due_reporting_month drm
         ON drm.asset_id = spv.asset_id
         AND drm.reporting_date::DATE = spv.reporting_date::DATE
      LEFT JOIN
         ods_spv_historical_test.due_lifetime dl
         ON dl.asset_id = spv.asset_id
         AND dl.reporting_date::DATE = spv.reporting_date::DATE
      LEFT JOIN
         prev_val prev
         ON prev.asset_id = spv.asset_id
      LEFT JOIN
         ods_spv_historical_test.asset_category acat
         ON spv.asset_id = acat.asset_id
      LEFT JOIN
         ods_spv_historical_test.asset_cohort_month chm
         ON spv.asset_id = chm.asset_id
      LEFT JOIN
         ods_spv_historical_test.customer_score cust_sco
         ON cust_sco.customer_id = COALESCE(h.customer_id::TEXT, s.customer_id::TEXT, cust.customer_id::TEXT, o.customer_id::TEXT)
      LEFT JOIN
         finance.asset_transferred_to_m_and_g atr
         ON spv.asset_id = atr.asset_id
   WHERE
      spv.reporting_date = '2023-07-31' ) ;
-----------------------------------------LUCXO REPORTING END--------------------------------------------------------------------------------------------
-----------------------------------------LUCXO REPORTING END--------------------------------------------------------------------------------------------

GRANT ALL ON ALL tables IN SCHEMA ods_spv_historical_test TO erkin_unler, radwa_hosny,
 nasim_pezeshki, saad_amir, ceyda_peker, pawan_pai;

GRANT SELECT ON ALL tables IN SCHEMA ods_spv_historical_test TO elene_tsintsadze,
 paolo_giambona, accounting_redash, basri_oz;
