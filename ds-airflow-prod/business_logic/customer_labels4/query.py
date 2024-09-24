import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd
import pandera as pa
from pandera.typing import Series
from sqlalchemy import text

from business_logic.customer_labels4.config import (CURRENT_TABLE_NAME,
                                                    HISTORICAL_TABLE_NAME,
                                                    REDSHIFT_SCHEMA,
                                                    TABLE_COLUMNS,
                                                    TEMP_TABLE_NAME)


class RowsSchema(pa.DataFrameModel):
    customer_id: Series[int] = pa.Field(nullable=True)
    trust_type: Series[object] = pa.Field(nullable=True)
    n_subscriptions: Series[float] = pa.Field(nullable=True)
    outstanding_assets: Series[float] = pa.Field(nullable=True)
    delivered_assets: Series[float] = pa.Field(nullable=True)
    n_revocations: Series[float] = pa.Field(nullable=True)
    n_active_subscriptions: Series[float] = pa.Field(nullable=True)
    avg_payment_dpd: Series[float] = pa.Field(nullable=True)
    max_payment_dpd: Series[float] = pa.Field(nullable=True)
    n_payments: Series[float] = pa.Field(nullable=True)
    last_payment_date: Series[object] = pa.Field(nullable=True)
    tot_revenue_due: Series[float] = pa.Field(nullable=True)
    n_past_failed_payments: Series[float] = pa.Field(nullable=True)
    n_paid_payments: Series[float] = pa.Field(nullable=True)
    time_since_first_payment: Series[float] = pa.Field(nullable=True)
    net_revenue_paid: Series[float] = pa.Field(nullable=True)
    time_to_first_failed_payment: Series[float] = pa.Field(nullable=True)
    outstanding_amount: Series[float] = pa.Field(nullable=True)
    total_refunded_amount: Series[float] = pa.Field(nullable=True)
    n_refunded_payments: Series[float] = pa.Field(nullable=True)
    current_dpd: Series[float] = pa.Field(nullable=True)
    revenue_vs_debt: Series[float] = pa.Field(nullable=True)
    time_since_last_payment: Series[float] = pa.Field(nullable=True)
    paid_rate: Series[float] = pa.Field(nullable=True)
    paid_not_refunded_payments: Series[float] = pa.Field(nullable=True)
    time_between_first_and_last_payment: Series[float] = pa.Field(nullable=True)
    n_dc_subscriptions: Series[float] = pa.Field(nullable=True)
    outstanding_dc_assets: Series[float] = pa.Field(nullable=True)
    asset_amount_paid: Series[float] = pa.Field(nullable=True)
    responsiveness: Series[object] = pa.Field(nullable=True)
    dc_state: Series[object] = pa.Field(nullable=True)
    failed_transaction_reasons: Series[object] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True


def query_customers_data(shipping_country, customer_type, batch_size = 1000):

    # Import subscriptions
    query_customers = f"""
        with customers as (
            select customer_id , trust_type
            from master.customer c
            where shipping_country = '{shipping_country}'
            and customer_type = '{customer_type}'
            and created_at < current_date
        ),
        subscriptions as (
            select
                s.customer_id,
                count(distinct s.subscription_id) as n_subscriptions,
                sum(s.outstanding_assets) as outstanding_assets,
                sum(coalesce(s.delivered_assets,0)) as delivered_assets,
                sum(case when s.cancellation_reason_new = 'REVOCATION' then 1 else 0 end) as n_revocations,
                sum(case when s.cancellation_date is null then 1 else 0 end) as n_active_subscriptions
            from master.subscription s
            join customers c
            on c.customer_id = s.customer_id
            join (
                select distinct subscription_id
                from master.subscription_payment
                ) as sp
            on sp.subscription_id = s.subscription_id
            group by s.customer_id
        ),
        subscription_payments as (
            select
                sp.customer_id,
                avg(dpd) as avg_payment_dpd,
                max(dpd) as max_payment_dpd,
                count(distinct payment_id) as n_payments,
                max(sp.paid_date) as last_payment_date,
                sum(sp.amount_due) as tot_revenue_due,
                sum(case when sp.dpd > 10 then 1 else 0 end) as n_past_failed_payments,
                sum(case when sp.paid_date is not null then 1 else 0 end) as n_paid_payments,
                max(datediff(day, sp.paid_date, current_date)) as time_since_first_payment,
                sum(sp.amount_paid) - sum(coalesce(amount_refund,0)) - sum(coalesce(amount_chargeback,0)) as net_revenue_paid,
                datediff(day, min(sp.due_date), min(case when sp.dpd> 15 then sp.due_date else null end)) as time_to_first_failed_payment,
                sum(sp.amount_due) - sum(coalesce(sp.amount_paid,0)) as outstanding_amount,
                sum(coalesce(sp.amount_refund,0)) + sum(coalesce(sp.amount_chargeback,0)) as total_refunded_amount,
                sum(case when sp.amount_refund is not null then 1 else 0 end) + sum(case when sp.amount_chargeback is not null then 1 else 0 end) as n_refunded_payments,
                max(case when sp.paid_date is null then sp.dpd else 0 end) as current_dpd
            from master.subscription_payment sp
            join customers c
            on c.customer_id = sp.customer_id
            join (
                select distinct subscription_id
                from master.subscription
                ) as s
            on sp.subscription_id = s.subscription_id
            where sp.status in ('PAID','FAILED','PARTIAL PAID','FAILED FULLY')
            and sp.due_date < current_date
            --and sp.asset_id is not null
            group by sp.customer_id
        ),
        debt_collection as (
            select
                s.customer_id,
                count(distinct s.subscription_id) as n_dc_subscriptions,
                sum(s.outstanding_assets) as outstanding_dc_assets
            from master.subscription s
            join customers c
            on c.customer_id = s.customer_id
            where cancellation_reason_new like '%DEBT COLLECTION%' or cancellation_reason like '%handed over to dca%'
            group by s.customer_id
        ),
        asset_payment as (
            select
                ap.customer_id,
                row_number() over (partition by ap.customer_id, ap.asset_id order by ap.due_date desc) as row_n,
                ap.amount_paid
            from master.asset_payment as ap
            join customers c
            on c.customer_id = ap.customer_id
            where ap.payment_type = 'CUSTOMER BOUGHT'
        ),
        assets as (
            select ap.customer_id, sum(ap.amount_paid) as asset_amount_paid
            from asset_payment ap
            where row_n=1
            group by ap.customer_id
        ),
        dc_input as (
            select
                s.customer_id,
                listagg(distinct lower(dc.responded), ',') as responsiveness,
                listagg(distinct lower(dc.current_state), ',') as dc_state,
                listagg(distinct lower(dc.last_failed_reason),',') as failed_transaction_reasons
            from dm_debt_collection.us_dc_customer_contact_retained dc
            join master.subscription s
            on s.subscription_id = dc.subscription_id
            group by s.customer_id
        )
        select
            c.customer_id,
            c.trust_type,
            s.n_subscriptions,
            s.outstanding_assets,
            s.delivered_assets,
            s.n_revocations,
            s.n_active_subscriptions,
            sp.avg_payment_dpd,
            sp.max_payment_dpd,
            sp.n_payments,
            sp.last_payment_date,
            sp.tot_revenue_due,
            sp.n_past_failed_payments,
            sp.n_paid_payments,
            sp.time_since_first_payment,
            sp.net_revenue_paid,
            sp.time_to_first_failed_payment,
            sp.outstanding_amount,
            sp.total_refunded_amount,
            sp.n_refunded_payments,
            sp.current_dpd,
            sp.net_revenue_paid - sp.outstanding_amount as revenue_vs_debt,
            datediff(day, sp.last_payment_date, current_date) as time_since_last_payment,
            case when (sp.tot_revenue_due - sp.total_refunded_amount) != 0 then sp.net_revenue_paid::float / (sp.tot_revenue_due - sp.total_refunded_amount) else null end as paid_rate,
            sp.n_paid_payments - sp.n_refunded_payments as paid_not_refunded_payments,
            sp.time_since_first_payment - time_since_last_payment as time_between_first_and_last_payment,
            dc.n_dc_subscriptions,
            dc.outstanding_dc_assets,
            a.asset_amount_paid,
            dc_input.responsiveness ,
            dc_input.dc_state,
            dc_input.failed_transaction_reasons
        from customers as c
        left join subscriptions as s
        on c.customer_id = s.customer_id
        left join subscription_payments as sp
        on sp.customer_id = s.customer_id
        left join debt_collection as dc
        on s.customer_id = dc.customer_id
        left join assets as a
        on s.customer_id = a.customer_id
        left join dc_input
        on s.customer_id = dc_input.customer_id
    """

    # redshift_engine = create_engine(db_url)
    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift')
    redshift_engine = rs.get_sqlalchemy_engine()

    with redshift_engine.connect() as connection:
        result = connection.execute(text(query_customers))
        for rows in iter(lambda: result.fetchmany(batch_size), []):
            rows = pd.DataFrame([dict(row) for row in rows])
            validated_rows = RowsSchema(rows)
            yield validated_rows


def create_redshift_tables():
    create_table_sql = [
        f"""
        DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{TEMP_TABLE_NAME};
        """
    ]
    for table_name in [TEMP_TABLE_NAME, CURRENT_TABLE_NAME, HISTORICAL_TABLE_NAME]:
        create_table_sql.append(
            f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{table_name} (
                customer_id INTEGER,
                label VARCHAR(32),
                rule VARCHAR(32),
                trust_type VARCHAR(32),
                rule_version VARCHAR(32),
                created_at TIMESTAMP
            );
            """
        )

    return create_table_sql


def update_historical_table(current_date):
    # Consider all the columns except the created_at column
    columns = ", ".join([col for col in TABLE_COLUMNS if col != "created_at"])

    update_query = [
        # Update historical table with new data
        f"""
        INSERT INTO {REDSHIFT_SCHEMA}.{HISTORICAL_TABLE_NAME} ({columns}, created_at)
        SELECT {columns}, '{current_date}'::timestamp AS created_at
        FROM (
            SELECT {columns}
            FROM {REDSHIFT_SCHEMA}.{TEMP_TABLE_NAME}
            MINUS
            SELECT {columns}
            FROM {REDSHIFT_SCHEMA}.{CURRENT_TABLE_NAME}
        )
        """,
        # Truncate the current table
        f"""
        TRUNCATE TABLE {REDSHIFT_SCHEMA}.{CURRENT_TABLE_NAME};
        """,
        # Insert the new data into the current table
        f"""
        INSERT INTO {REDSHIFT_SCHEMA}.{CURRENT_TABLE_NAME}
        SELECT * FROM {REDSHIFT_SCHEMA}.{TEMP_TABLE_NAME};
        """,
        # Drop temp table
        f"""
        DROP TABLE {REDSHIFT_SCHEMA}.{TEMP_TABLE_NAME};
        """
    ]

    return update_query
