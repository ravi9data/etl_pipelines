import datetime
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from plugins.dag_utils import get_schedule, on_failure_callback

DAG_ID = "staging_tables_inc_load"

logger = logging.getLogger(__name__)

default_args = {
    "owner": "bi-eng",
    "depends_on_past": False,
    "start_date": datetime.datetime(2022, 11, 9),
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=15),
    "on_failure_callback": on_failure_callback,
    "execution_timeout": datetime.timedelta(minutes=30),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="curation layer for all cdc tables",
    schedule_interval=get_schedule("0 22 * * *"),
    max_active_runs=1,
    catchup=False,
    tags=["staging", DAG_ID, "curation"],
)

customer_contracts = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="customer_contracts",
    sql="./sql/customer_contracts.sql",
)

fulfillment_eu_shipment_update = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="fulfillment_eu_shipment_update_v1",
    sql="./sql/fulfillment_eu_shipment_update_v1.sql",
)

operations_orders_allocated = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="operations_orders_allocated",
    sql="./sql/operations_orders_allocated.sql",
)

operations_replacement_allocated = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="operations_replacement_allocated",
    sql="./sql/operations_replacement_allocated.sql",
)

orders_cancelled = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="orders_cancelled",
    sql="./sql/orders_cancelled.sql",
)

orders_placed = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="orders_placed",
    sql="./sql/orders_placed.sql",
)

poll_votes = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="poll_votes",
    sql="./sql/poll_votes.sql",
)

shipcloud_incoming_notifications = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="shipcloud_incoming_notifications",
    sql="./sql/shipcloud_incoming_notifications.sql",
)

shipment_changes = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="shipment_changes",
    sql="./sql/shipment_changes.sql",
)

shipment_inbound = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="shipment_inbound",
    sql="./sql/shipment_inbound.sql",
)

# internal_billing_payments = PostgresOperator(
#         dag=dag,
#         postgres_conn_id='redshift_default',
#         task_id="internal_billing_payments",
#         sql="./sql/internal_billing_payments.sql"
#     )

scoring_customer_fraud_check_completed = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="scoring_customer_fraud_check_completed",
    sql="./sql/scoring_customer_fraud_check_completed.sql",
)

b2b_eu_dashboard_access_activated_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="b2b_eu_dashboard_access_activated_v1",
    sql="./sql/b2b_eu_dashboard_access_activated_v1.sql",
)

grover_card_reservation = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="grover_card_reservation",
    sql="./sql/grover_card_reservations.sql",
)

checkout_eu_cart_created_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="checkout_eu_cart_created_v1",
    sql="./sql/checkout_eu_cart_created_v1.sql",
)

stg_order_approval = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="stg_order_approval",
    sql="./sql/stg_order_approval.sql",
)

stg_api_production_adyen = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="stg_api_production_adyen",
    sql="./sql/adyen_payment_requests.sql",
)

payment_methods_mt = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="payment_methods_mt",
    sql="./sql/payment_methods.sql",
)

addons_submitted_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="addons_submitted_v1",
    sql="./sql/addons_submitted_v1.sql",
)

addons_order_status_change_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="addons_order_status_change_v1",
    sql="./sql/addons_order_status_change_v1.sql",
)

addons_paid_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="addons_paid_v1",
    sql="./sql/addons_paid_v1.sql",
)

addons_activation_code_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="addons_activation_code_v1",
    sql="./sql/addons_activation_code_v1.sql",
)

checkout_eu_us_cart_orders_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="checkout_eu_us_cart_orders_v1",
    sql="./sql/checkout_eu_us_cart_orders_v1.sql",
)

order_scoring_spectrum_tables = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="order_scoring_spectrum_tables",
    sql="./sql/order_scoring_spectrum_tables.sql",
)

customer_scoring_spectrum_tables = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="customer_scoring_spectrum_tables",
    sql="./sql/customer_scoring_spectrum_tables.sql",
)

checkout_eu_us_cart_orders_updated_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="checkout_eu_us_cart_orders_updated_v1",
    sql="./sql/checkout_eu_us_cart_orders_updated_v1.sql",
)

legacy_grover_care_order_paid_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="legacy_grover_care_order_paid_v1",
    sql="./sql/legacy_grover_care_order_paid_v1.sql",
)

legacy_grover_care_order_submitted_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="legacy_grover_care_order_submitted_v1",
    sql="./sql/legacy_grover_care_order_submitted_v1.sql",
)

referral_eu_code_used_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="referral_eu_code_used_v1",
    sql="./sql/referral_eu_code_used_v1.sql",
)

b2b_eu_dashboard_access_activated_v2 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="b2b_eu_dashboard_access_activated_v2",
    sql="./sql/b2b_eu_dashboard_access_activated_v2.sql",
)

referral_eu_guest_item_returned_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="referral_eu_guest_item_returned_v1",
    sql="./sql/referral_eu_guest_item_returned_v1.sql",
)

referral_eu_host_invitation_fulfilled_v2 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="referral_eu_host_invitation_fulfilled_v2",
    sql="./sql/referral_eu_host_invitation_fulfilled_v2.sql",
)

referral_eu_guest_contract_started_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="referral_eu_guest_contract_started_v1",
    sql="./sql/referral_eu_guest_contract_started_v1.sql",
)

grover_card_seizures = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="grover_card_seizures",
    sql="./sql/grover_card_seizures.sql",
)

internal_loyalty_service_transactions_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="internal_loyalty_service_transactions_v1",
    sql="./sql/internal_loyalty_service_transactions_v1.sql",
)

internal_loyalty_service_actions_queue = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="internal_loyalty_service_actions_queue",
    sql="./sql/internal_loyalty_service_actions_queue.sql",
)

internal_loyalty_service_credits = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="internal_loyalty_service_credits",
    sql="./sql/internal_loyalty_service_credits.sql",
)

billing_invoices = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="billing_invoices",
    sql="./sql/billing_invoices.sql",
)

catalog_rental_plans_price_change_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="catalog_rental_plans_price_change_v1",
    sql="./sql/catalog_rental_plans_price_change_v1.sql",
)

risk_users_queue = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="risk_users_queue",
    sql="./sql/risk_users_queue.sql",
)

risk_eu_order_decision_intermediate_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="risk_eu_order_decision_intermediate_v1",
    sql="./sql/risk_eu_order_decision_intermediate_v1.sql",
)

risk_internal_eu_manual_review_result_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="risk_internal_eu_manual_review_result_v1",
    sql="./sql/risk_internal_eu_manual_review_result_v1.sql",
)

risk_internal_us_manual_review_result_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="risk_internal_us_manual_review_result_v1",
    sql="./sql/risk_internal_us_manual_review_result_v1.sql",
)

risk_eu_id_verification_order = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="risk_eu_id_verification_order",
    sql="./sql/risk_eu_id_verification_order.sql",
)

stg_internal_billing_payments = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="stg_internal_billing_payments",
    sql="./sql/stg_internal_billing_payments.sql",
)

stg_risk_eu_order_decision_final_v1 = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="stg_risk_eu_order_decision_final_v1",
    sql="./sql/stg_risk_eu_order_decision_final_v1.sql",
)

order_additional_info = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="order_additional_info",
    sql="./sql/order_additional_info.sql",
)

(
    [
        order_scoring_spectrum_tables,
        customer_scoring_spectrum_tables,
        checkout_eu_us_cart_orders_updated_v1,
        legacy_grover_care_order_paid_v1,
        legacy_grover_care_order_submitted_v1,
    ]
    >> referral_eu_code_used_v1
    >> [
        b2b_eu_dashboard_access_activated_v2,
        referral_eu_guest_item_returned_v1,
        referral_eu_host_invitation_fulfilled_v2,
        referral_eu_guest_contract_started_v1,
        grover_card_seizures,
    ]
    >> internal_loyalty_service_transactions_v1
    >> [
        internal_loyalty_service_actions_queue,
        internal_loyalty_service_credits,
        billing_invoices,
        catalog_rental_plans_price_change_v1,
    ]
    >> risk_users_queue
    >> [
        risk_eu_order_decision_intermediate_v1,
        risk_internal_eu_manual_review_result_v1,
        risk_internal_us_manual_review_result_v1,
        customer_contracts,
        fulfillment_eu_shipment_update,
        risk_eu_id_verification_order,
        stg_risk_eu_order_decision_final_v1,
        order_additional_info
    ]
    >> operations_orders_allocated
    >> [
        operations_replacement_allocated,
        orders_cancelled,
        orders_placed,
        poll_votes,
        shipcloud_incoming_notifications,
    ]
    >> shipment_changes
    >> [
        shipment_inbound,
        stg_internal_billing_payments,
        scoring_customer_fraud_check_completed,
    ]
    >> b2b_eu_dashboard_access_activated_v1
    >> [
        grover_card_reservation,
        checkout_eu_cart_created_v1,
        stg_order_approval,
        stg_api_production_adyen,
    ]
    >> payment_methods_mt
    >> [
        addons_submitted_v1,
        addons_order_status_change_v1,
        addons_paid_v1,
        addons_activation_code_v1,
        checkout_eu_us_cart_orders_v1,
    ]
)
