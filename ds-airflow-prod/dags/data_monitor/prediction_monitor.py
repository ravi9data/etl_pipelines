import datetime
import logging
from typing import Tuple

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from dateutil.relativedelta import relativedelta
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from scipy.stats import mannwhitneyu
from slack_sdk import WebClient
from statsmodels.stats.contingency_tables import mcnemar

from dags.data_monitor.shared_tools import (calc_js_distance, get_engine,
                                            get_model_ids, get_resampled)
from plugins.utils.sql import read_sql_file

REDSHIFT_CONN_ID = "redshift"
POSTGRES_CONN_ID = "postgres_replica"
TABLE_NAME_PREDS = "prediction_monitor"
SCHEMA_NAME = "data_science_dev"


def write_to_db(df: pd.DataFrame) -> None:
    engine = get_engine(REDSHIFT_CONN_ID)

    df.to_sql(
        method="multi",
        name=TABLE_NAME_PREDS,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="append",
        index=False,
    )


def get_channel_info() -> Tuple[str, str]:
    """
    Return channel id and token of slack_token variable.
    """
    var = Variable.get("slack_token", deserialize_json=True)
    return var["id"], var["token"]


def prepare_pred_data(
    model_id: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp, pd.Timestamp]:
    QUERY = read_sql_file("./dags/data_monitor/sql/prediction_data.sql").format(
        model_id=model_id
    )

    conn = get_engine(POSTGRES_CONN_ID)

    df = pd.read_sql(QUERY, conn)

    if df.empty:
        logging.error("Dataframe is empty.")

    df = df.drop(columns=["model_id"])

    ref_start_date = df["creation_timestamp"].min()
    ref_end_date = ref_start_date + relativedelta(months=1)

    split_date = str(ref_end_date)

    base_pred = df.set_index("creation_timestamp").loc[:split_date].reset_index()
    post_pred = df.set_index("creation_timestamp").loc[split_date:].reset_index()

    return base_pred, post_pred, ref_start_date, ref_end_date


def prepare_label_data(model_id: int) -> pd.DataFrame:
    QUERY = read_sql_file("./dags/data_monitor/sql/labels.sql").format(
        model_id=model_id
    )

    conn = get_engine(POSTGRES_CONN_ID)

    df = pd.read_sql(QUERY, conn)
    df = pd.get_dummies(df, columns=["score_label", "labels3"])
    df = df.replace({"good": False, "fraud": True})
    df = make_cm(df)
    df = df.set_index("creation_timestamp").resample("W-MON").apply(lambda x: np.sum(x))

    return df


def plot_df(
    df: pd.DataFrame,
    model_id: int,
    x_label: str = "",
    y_label: str = "",
    title: str = "",
    col_name: str = "pvalue",
) -> None:
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(10, 6))
    canvas = FigureCanvas(fig)

    ax.plot_date(df.index, df[col_name].values, "v-")
    ax.tick_params(axis="x", labelrotation=30)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(f"{title} model {model_id}")
    if col_name in ("pvalue", "mw_test_pval"):
        ax.set_ylim(0, 1)
        ax.axhline(y=0.05, label="0.05 threshold", c="green", ls="--")
    fig.tight_layout()

    filepath = Variable.get("file_path")
    filepath1 = f"{filepath}{title.replace(' ', '_')}_{model_id}.png"

    canvas.draw()
    width, height = fig.get_size_inches() * fig.get_dpi()
    image = np.frombuffer(canvas.tostring_rgb(), dtype="uint8").reshape(
        int(height), int(width), 3
    )

    with open(filepath1, "wb") as f:
        plt.imsave(f, image)

    post_to_slack(filepath1)


def make_cm(df: pd.DataFrame) -> pd.DataFrame:
    ones = pd.Series([1] * df.shape[0])
    df["true_true"] = (df["score_label_good"] == ones) & (
        df["labels3_good"] == ones
    ).astype(int)
    df["false_false"] = (df["score_label_fraud"] == ones) & (
        df["labels3_fraud"] == ones
    ).astype(int)
    df["true_false"] = (df["score_label_good"] == ones) & (
        df["labels3_fraud"] == ones
    ).astype(int)
    df["false_true"] = (df["score_label_fraud"] == ones) & (
        df["labels3_good"] == ones
    ).astype(int)

    df["true_true"] = df["true_true"].astype(int)

    df["cm"] = df.apply(
        lambda x: np.array(
            [[x["true_true"], x["false_true"]], [x["true_false"], x["false_false"]]]
        ),
        axis=1,
    )

    df = df.filter(items=["creation_timestamp", "cm"])

    return df


def post_to_slack(filepath1: str) -> None:
    CHANNEL_ID, SLACK_TOKEN = get_channel_info()
    client = WebClient(token=SLACK_TOKEN)
    client.files_upload(channels=CHANNEL_ID, file=filepath1)


model_ids = get_model_ids()
for model_id in model_ids:
    dag_id = f"prediction_monitor_model_{model_id}"

    @dag(
        dag_id=dag_id,
        start_date=datetime.datetime(2020, 2, 2),
        schedule_interval="0 0 * * 0",
        catchup=False,
        default_args={"retries": 2, "retry_delay": datetime.timedelta(minutes=10)},
        tags=["monitoring"],
    )
    def dynamic_dag():
        @task
        def mcnemar_test(model_id: int) -> str:
            data = prepare_label_data(model_id)
            data["mn_test_pval"] = data.apply(lambda x: mcnemar(x["cm"]).pvalue, axis=1)
            data = data.filter(items=["mn_test_pval"])

            plot_df(
                data,
                model_id,
                "Date",
                "$p_{value}$",
                title="McNemar Test for labels",
                col_name="mn_test_pval",
            )

            data = data.reset_index()
            data = data.rename(columns={"creation_timestamp": "split_date"})
            data["split_date"] = data["split_date"].astype(str)

            return data.to_json()

        @task
        def mann_whitney_test(model_id: int) -> str:
            ref_data, post_data, _, _ = prepare_pred_data(model_id)

            if ref_data.empty:
                logging.info("No enough data")
                return "No enough data"

            post_data = get_resampled(post_data)
            post_data["mw_test_pval"] = post_data.apply(
                lambda x: mannwhitneyu(ref_data["score"].values, x["score"]).pvalue,
                axis=1,
            )
            post_data = post_data.filter(items=["mw_test_pval"])

            plot_df(
                post_data,
                model_id,
                "Date [1 week]",
                "$p_{value}$",
                "Mann-Whitney U test for prediction scores [1W]",
                col_name="mw_test_pval",
            )

            post_data = post_data.reset_index()
            post_data = post_data.rename(columns={"creation_timestamp": "split_date"})
            post_data["split_date"] = post_data["split_date"].astype(str)

            return post_data.to_json()

        @task
        def jensen_shannon_test(model_id: int) -> str:
            ref_data, post_data, _, _ = prepare_pred_data(model_id)

            if ref_data.empty:
                logging.info("No enough data")
                return "No enough data"

            post_data = get_resampled(post_data)

            if post_data.shape[0] < 10:
                logging.info(f"Not enough data: {post_data.shape[0]}")
                return "No enough data"

            ref_hist_vals, ref_hist_bins, _ = plt.hist(
                ref_data["score"].values, bins="sqrt"
            )

            post_data = post_data.apply(
                calc_js_distance, args=(ref_hist_vals, ref_hist_bins), axis=1
            )
            post_data = post_data.drop(columns=["score"])

            plot_df(
                post_data,
                model_id=model_id,
                x_label="Date",
                y_label="Distance",
                title="Jensen-Shannnon distance for prediction scores",
                col_name="js_distance",
            )

            post_data = post_data.reset_index()
            post_data = post_data.rename(columns={"creation_timestamp": "split_date"})
            post_data["split_date"] = post_data["split_date"].astype(str)

            return post_data.to_json()

        @task
        def delete_table_preds(model_id: int) -> None:
            engine = get_engine(REDSHIFT_CONN_ID)

            DELETE_QUERY = read_sql_file(
                "./dags/data_monitor/sql/delete_table_model.sql"
            ).format(
                schema_name=SCHEMA_NAME,
                table_name_preds=TABLE_NAME_PREDS,
                model=model_id,
            )
            #  pg_hook.run(DELETE_QUERY)
            engine.execute(DELETE_QUERY)
            logging.info(f"Deleted content of {TABLE_NAME_PREDS}.")

        @task
        def write_db(model_id: int, **kwargs) -> None:
            task_id = "mann_whitney_test"
            task_instance = kwargs["ti"]
            value = task_instance.xcom_pull(key="return_value", task_ids=task_id)
            df_mw = pd.read_json(value)

            task_id = "mcnemar_test"
            task_instance = kwargs["ti"]
            value = task_instance.xcom_pull(key="return_value", task_ids=task_id)
            df_mn = pd.read_json(value)

            task_id = "jensen_shannon_test"
            task_instance = kwargs["ti"]
            value = task_instance.xcom_pull(key="return_value", task_ids=task_id)
            df_js = pd.read_json(value)

            _, _, ref_start_date, ref_end_date = prepare_pred_data(model_id)

            final_df = pd.merge(df_mw, df_mn, on="split_date", how="outer")
            final_df = pd.merge(final_df, df_js, on="split_date", how="outer")
            final_df["model_id"] = model_id
            final_df["ref_start_date"] = ref_start_date
            final_df["ref_end_date"] = ref_end_date

            write_to_db(final_df)

        (
            [
                mcnemar_test(model_id),
                mann_whitney_test(model_id),
                jensen_shannon_test(model_id),
            ]
            >> delete_table_preds(model_id)
            >> write_db(model_id)
        )

    globals()[dag_id] = dynamic_dag()
