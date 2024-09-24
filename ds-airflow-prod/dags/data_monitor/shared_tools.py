import logging
import re
from typing import Any

import matplotlib.pyplot as plt
import numpy.typing as npt
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from scipy.spatial.distance import jensenshannon

from plugins.utils.sql import read_sql_file

POSTGRES_CONN_ID = "postgres_replica"


def get_json_columns(model_id: int) -> str:
    QUERY = read_sql_file("./dags/data_monitor/sql/feature_list.sql").format(
        model_id=model_id
    )
    conn = get_engine(POSTGRES_CONN_ID)

    model_features = pd.read_sql(QUERY, conn)["feats"].values[0]

    if not model_features:
        logging.error(f"Model with id {model_id} doesn't have feature list.")

    return ",".join(
        [f"json_content->'{i}' AS {re.sub(r' |-|&', '_', i)}" for i in model_features]
    )


def calc_js_distance(
    row: pd.Series,
    ref_hist_vals: npt.NDArray,
    ref_hist_bins: npt.NDArray,
    col_name: str = "score",
) -> pd.Series:
    post_hist_vals, _, _ = plt.hist(row[col_name], bins=ref_hist_bins)
    row["js_distance"] = jensenshannon(ref_hist_vals, post_hist_vals)

    return row


def get_resampled(post_pred: pd.DataFrame, frequency: str = "W-MON") -> pd.DataFrame:
    return post_pred.set_index("creation_timestamp").resample(frequency).agg(list)


def get_model_mapping() -> dict:
    QUERY = read_sql_file("./dags/data_monitor/sql/model_map.sql")
    pg_hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
    )

    conn = pg_hook.get_conn()
    mapping_df = pd.read_sql(QUERY, conn)

    return {j[0]: j[1] for _, j in mapping_df.iterrows()}


def get_engine(connection_id: str) -> Any:
    return PostgresHook(
        postgres_conn_id=connection_id,
    ).get_sqlalchemy_engine()


def get_model_ids() -> list:
    return Variable.get("model_ids", deserialize_json=True, default_var=[])
