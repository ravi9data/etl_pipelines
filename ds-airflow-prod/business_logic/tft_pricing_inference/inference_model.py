import logging
import os
import pickle
import warnings
from datetime import datetime
from typing import List, Tuple

import awswrangler as wr
import boto3
import holidays
import mlflow
import pandas as pd
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from mlflow.tracking import MlflowClient
from packaging.specifiers import SpecifierSet
from packaging.version import Version
from pandas.api.types import is_list_like

from business_logic.tft_pricing_inference.variables import (
    bucket_name, data_limit_to_20_runs, final_models_path,
    inference_models_path, pricing_schema, pricing_table)
from plugins.utils.connections import redshift_conn
from plugins.utils.df_tools import _get_32_bit_dtype
from plugins.utils.s3_utils import current_path

N_POINTS = 20

# change days to 14+4 days when data source is switched to athena
DATETIME_NOW = pd.Timestamp(datetime.now())
datetime_past = DATETIME_NOW - pd.DateOffset(days=49)

TRACKING_URI = "https://mlflow.eu-production.internal.grover.com/"
MODEL_NAME = "TFTmodel_NegativeBinomialDistributionLoss"

allowed_agg_funcs = ["mean", "max", "min", "std"]

de_holidays = holidays.DE()

# Version check if pandas version is the production version
prod_version_set = SpecifierSet("~=1.3.5")  # Version frpom the production
if Version(pd.__version__) in prod_version_set:
    is_prod_pandas = True
else:
    is_prod_pandas = False


def add_rolling_features(
    df: pd.DataFrame,
    rolls: List[int],
    column: str,
    agg_funcs: List[str] = ["mean", "std"],
    ts_id: str = None,
    n_shift: int = 1,
    use_32_bit: bool = False,
):
    """
    Add rolling statistics from the column provided and adds them as other
        columns in the provided dataframe

    Args:
        df (pd.DataFrame): The dataframe in which features needed to be created
        rolls (List[int]): Different windows over which the rolling
            aggregations to be done
        column (str): The column used for feature engineering
        agg_funcs (List[str], optional): The different aggregations to be
            done on the rolling window. Defaults to ["mean", "std"].
        ts_id (str, optional): Unique id for a time series. Defaults to None.
        n_shift (int, optional): Number of time steps to shift before computing
            rolling statistics.
            Typically used to avoid data leakage. Defaults to 1.
        use_32_bit (bool, optional): Flag to use float32 or int32 to reduce
            memory. Defaults to False.
    Returns:
        Tuple[pd.DataFrame, List]: Returns a tuple of the new dataframe and
            a list of features which were added
    """
    assert is_list_like(
        rolls
    ), "`rolls` should be a list of all required rolling windows"
    assert (
        column in df.columns
    ), "`column` should be a valid column in the provided dataframe"
    assert (
        len(set(agg_funcs) - set(allowed_agg_funcs)) == 0
    ), f"`agg_funcs` should be one of {allowed_agg_funcs}"
    _32_bit_dtype = _get_32_bit_dtype(df[column])
    if ts_id is None:
        warnings.warn(
            "Assuming just one unique time series in dataset. "
            "If there are multiple, provide `ts_id` argument"
        )
        # Assuming just one unique time series in dataset
        rolling_df = [
            df.groupby("snapshot_date")[column]
            .apply(int)
            .groupby(level=0)
            .shift(n_shift)
            .rolling(i)
            .agg({f"{column}_rolling_{i}_{agg}": agg for agg in agg_funcs})
            .reset_index()
            for i in rolls
        ]

    else:
        if ts_id in df.columns:
            rolling_df = [
                df.groupby([ts_id, "snapshot_date"])[column]
                .apply(int)
                .groupby(level=0)
                .shift(n_shift)
                .rolling(i)
                .agg({f"{column}_rolling_{i}_{agg}": agg for agg in agg_funcs})
                .reset_index()
                for i in rolls
            ]
        else:
            print("`ts_id` should be a valid column in the provided dataframe")

    added_features = []
    for roll_df in rolling_df:
        df = pd.merge(df, roll_df, on=["product_sku", "snapshot_date"], how="left")
        added_features += [
            col for col in roll_df if col not in ("product_sku", "snapshot_date")
        ]

    if use_32_bit and _32_bit_dtype is not None:
        df[added_features] = df[added_features].astype(_32_bit_dtype)

    return df, added_features


def add_lags(
    df: pd.DataFrame,
    lags: List[int],
    column: str,
    ts_id: str = None,
    use_32_bit: bool = False,
) -> Tuple[pd.DataFrame, List]:
    """
    Create Lags for the column provided and adds them as
    other columns in the provided dataframe

    Args:
        df (pd.DataFrame): The dataframe in which features needed to be created
        lags (List[int]): List of lags to be created
        column (str): Name of the column to be lagged
        ts_id (str, optional): Column name of Unique ID of a time series to be
            grouped by before applying the lags.
            If None assumes dataframe only has a single timeseries.
            Defaults to None.
        use_32_bit (bool, optional): Flag to use float32 or int32 to
            reduce memory. Defaults to False.

    Returns:
        Tuple(pd.DataFrame, List): Returns a tuple of the new dataframe and
            a list of features which were added
    """
    assert is_list_like(lags), "`lags` should be a list of all required lags"
    assert (
        column in df.columns
    ), "`column` should be a valid column in the provided dataframe"
    _32_bit_dtype = _get_32_bit_dtype(df[column])
    if ts_id is None:
        warnings.warn(
            "Assuming just one unique time series in dataset. "
            "If there are multiple, provide `ts_id` argument"
        )
        # Assuming just one unique time series in dataset
        if use_32_bit and _32_bit_dtype is not None:
            col_dict = {
                f"{column}_lag_{i}": df[column].shift(i).astype(_32_bit_dtype)
                for i in lags
            }
        else:
            col_dict = {f"{column}_lag_{i}": df[column].shift(i) for i in lags}
    else:
        assert (
            ts_id in df.columns
        ), "`ts_id` should be a valid column in the provided dataframe"
        if use_32_bit and _32_bit_dtype is not None:
            col_dict = {
                f"{column}_lag_{i}": df.groupby([ts_id])[column]
                .shift(i)
                .astype(_32_bit_dtype)
                for i in lags
            }
        else:
            col_dict = {
                f"{column}_lag_{i}": df.groupby([ts_id])[column].shift(i) for i in lags
            }
    df = df.assign(**col_dict)
    added_features = list(col_dict.keys())
    return df, added_features


def customize_features(df, val_discount_12m=0.0):
    df_1 = df.copy()
    df_1.loc[:, "discount_12m"] = val_discount_12m
    df_1.loc[:, "active_price_12m"] = (
        1 - df_1.loc[:, "discount_12m"] / 100
    ) * df_1.loc[:, "high_price_12m"]

    return df_1


def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucketName)
    for obj in bucket.objects.filter(Prefix=remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        logging.info(f"Downloading file: {obj.key}")
        bucket.download_file(obj.key, obj.key)


def prepare_decoder_dataset(last_data, max_prediction_length):
    # forward fill last data
    decoder_data = pd.concat(
        [
            last_data.assign(snapshot_date=lambda x: x.snapshot_date + pd.DateOffset(i))
            for i in range(1, max_prediction_length + 1)
        ],
        ignore_index=True,
    )

    # Adjusting time features

    decoder_data["month"] = (
        decoder_data["snapshot_date"].dt.month.astype(str).astype("category")
    )
    decoder_data["day"] = (
        decoder_data["snapshot_date"].dt.day.astype(str).astype("category")
    )
    decoder_data["weekday"] = (
        decoder_data["snapshot_date"].dt.weekday.astype(str).astype("category")
    )

    # not sure if it is correct
    decoder_data["holiday"] = decoder_data.apply(
        lambda x: int(x.snapshot_date in de_holidays), axis=1
    )
    decoder_data["holiday"] = decoder_data["holiday"].astype(str).astype("category")

    decoder_data["time_idx"] = (
        decoder_data["snapshot_date"] - min(decoder_data["snapshot_date"])
    ).dt.days

    return decoder_data


def prepare_prediction_data(encoder_data, decoder_data):
    decoder_data["time_idx"] += encoder_data["time_idx"].max() + 1

    new_prediction_data = pd.concat([encoder_data, decoder_data], ignore_index=True)

    return new_prediction_data


def make_preds(prediction_data, loaded_model):
    results = {}
    new_raw_predictions, new_x, new_idx = loaded_model.predict(
        prediction_data, mode="prediction", return_x=True, return_index=True
    )

    for i in range(new_idx.shape[0]):
        preds = new_raw_predictions[i].numpy().tolist()
        results[new_idx.product_sku[i]] = preds

    df = pd.DataFrame.from_dict(
        results,
        orient="index",
        columns=[
            "pred_day_1",
            "pred_day_2",
            "pred_day_3",
            "pred_day_4",
            "pred_day_5",
            "pred_day_6",
            "pred_day_7",
        ],
    ).reset_index()

    df = df.rename(columns={"index": "product_sku"})
    df["total_preds"] = df[
        [
            "pred_day_1",
            "pred_day_2",
            "pred_day_3",
            "pred_day_4",
            "pred_day_5",
            "pred_day_6",
            "pred_day_7",
        ]
    ].sum(axis=1)

    return df


def add_columns(df, last_data):
    df = pd.merge(
        df,
        last_data[
            ["product_sku", "discount_12m", "high_price_12m", "active_price_12m"]
        ],
        on="product_sku",
        how="left",
    )

    return df


def run_tft_price_elasticity():
    # dynamically change the output dir to Monday of every week
    final_models_full_path = f"s3://{bucket_name}{final_models_path}".format(
        bucket_name=bucket_name, final_models_path=final_models_path
    )
    current_final_models_full_path = current_path(final_models_full_path)
    logging.info(f"current_final_models_full_path: {current_final_models_full_path}")

    if is_prod_pandas:
        data = wr.s3.read_csv(
            current_final_models_full_path, parse_dates=["snapshot_date"],
            infer_datetime_format=True
        )
    else:
        data = wr.s3.read_csv(
            current_final_models_full_path, parse_dates=["snapshot_date"], date_format="%Y-%m-%d"
        )

    # We would redefine it later on
    data = data.drop(columns=["utilisation"], axis=1)

    logging.info(f"Fetched data min date: {data.snapshot_date.min()}")
    logging.info(f"Fetched data max date: {data.snapshot_date.max()}")

    data = data.set_index("snapshot_date")

    # data for inference, 14 days of history data
    data = data[str(datetime_past): str(DATETIME_NOW)]
    logging.info(f"Stripped data size {data.shape}")
    logging.info(f"Stripped data min date: {data.index.min()}")
    logging.info(f"Stripped data max date: {data.index.max()}")
    data = data.sort_index()

    data = data.reset_index()

    # defining utilisation
    data.loc[:, "utilisation"] = data["active_subs"] / (
        data["active_subs"] + data["stock_on_hand"]
    )
    data["utilisation"] = data["utilisation"].fillna(0)

    # drop fitness category
    df_data = data[~(data["category_name"] == "Fitness")]
    df_data = df_data.reset_index(drop=True)

    df_data, added_rolling_feat = add_rolling_features(
        df_data,
        rolls=[4, 6],
        column="total_orders",
        agg_funcs=["mean"],
        ts_id="product_sku",
        n_shift=1,
        use_32_bit=False,
    )

    logging.info(f"Added features: [{', '.join([i for i in added_rolling_feat])}]")

    df_data, added_lags_feat = add_lags(
        df_data,
        lags=[1, 2, 3, 4],
        column="total_orders",
        ts_id="product_sku",
        use_32_bit=False,
    )

    logging.info(f"Added features: [{', '.join([i for i in added_lags_feat])}]")

    # Taking care of nulls
    df_data[added_rolling_feat] = df_data[added_rolling_feat].fillna(0)
    df_data[added_lags_feat] = df_data[added_lags_feat].fillna(0)

    logging.info("Establishing dataframe")
    # Drop rows with NaN in discount_12m - indicates 0 prices
    df_data = df_data.dropna(subset=["discount_12m"])
    # Convert date to datetime object
    df_data["snapshot_date"] = pd.to_datetime(df_data["snapshot_date"])

    df_data["discount_12m_orig"] = df_data["discount_12m"]
    df_data["discount_12m"] = 100.0 - df_data["discount_12m"]
    # Get minimum date in dataframe
    start_date = min(df_data["snapshot_date"])
    # Construct an index based on number of days
    df_data["time_idx"] = (df_data["snapshot_date"] - start_date).dt.days - 4

    # Generate a 'weekday' entry and convert to string categorical data
    df_data["weekday"] = df_data["snapshot_date"].dt.weekday

    category_list = [
        "month",
        "day",
        "holiday",
        "weekday",
        "product_sku",
        "category_name",
        "subcategory_name",
        "brand",
        "is_campaign",
    ]

    df_data[category_list] = df_data[category_list].astype(str).astype("category")

    # Additional variable
    df_data["avg_orders_per_sku"] = df_data.groupby(["product_sku"], observed=True)[
        "total_orders"
    ].transform("mean")
    df_data["avg_daily_orders_per_brand"] = df_data.groupby(
        ["snapshot_date", "subcategory_name", "brand"], observed=True
    )["total_orders"].transform("mean")
    df_data["sum_daily_orders_per_subcat"] = df_data.groupby(
        ["snapshot_date", "subcategory_name"], observed=True
    )["total_orders"].transform("sum")

    float_list = [
        "total_orders",
        "discount_12m",
        "high_price_12m",
        "active_price_12m",
        "views",
        "uniqviews",
        "utilisation",
        "stock_on_hand",
        "EU Petrol",
        "sum_daily_orders_per_subcat",
    ]
    # Convert num_orders to float
    df_data[float_list] = df_data[float_list].astype(float)

    prediction_length = 7
    encoder_length = 14

    #  min_prediction_length = prediction_length
    max_prediction_length = prediction_length
    # Encoder length      : number of historical days to consider
    #  min_encoder_length = encoder_length
    max_encoder_length = encoder_length
    # Minimum length      : minimum number of days required per sku
    #  minimum_length = prediction_length + encoder_length

    encoder_data = df_data[lambda x: x.time_idx > x.time_idx.max() - max_encoder_length]

    logging.info("Encoder data below")

    logging.info(f"Encoder min date: {encoder_data.snapshot_date.min()}")
    logging.info(f"Encoder max date: {encoder_data.snapshot_date.max()}")

    last_data = df_data[lambda x: x.time_idx == x.time_idx.max()]

    step = 100 // N_POINTS
    lst_last_data = []
    for val in range(0, 100, step):
        res = customize_features(last_data, val_discount_12m=val)
        lst_last_data.append(res)

    # fetching latest registered model in Production
    client = MlflowClient(TRACKING_URI)
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment("Pricing")
    latest_models = client.get_latest_versions(MODEL_NAME)

    #  Fetching all models from a given experiment and selecting the one
    # which is in the Production
    # We assume that there can be only one model in the production
    # for a given experiment
    for m in latest_models:
        if m.current_stage == "Production":
            model_path = m.source

    split_path = model_path.split("/")
    s3_host = split_path[2]
    s3_path = "/".join(split_path[3:-1])

    logging.info(f"We fetched model {s3_path!r}")

    downloadDirectoryFroms3(
        s3_host,
        s3_path,
    )

    # loading didn't fucntion so we just use pickle to open the model.
    with open(
        f"{s3_path}/model_pickle/TFTmodel_NegativeBinomialDistributionLoss.pckl",
        "rb",
    ) as file:
        loaded_model = pickle.load(file)
    logging.info(loaded_model)

    list_dataframes = []
    for last_data in lst_last_data:
        df_dec = prepare_decoder_dataset(last_data, max_prediction_length)
        df_pred_data = prepare_prediction_data(encoder_data, df_dec)
        df_preds = make_preds(df_pred_data, loaded_model)
        df_res = add_columns(df_preds, last_data)

        list_dataframes.append(df_res)

    df_final = pd.concat(list_dataframes)
    df_final["model_run_id"] = f"{s3_path.split('/')[1]}"

    inference_models_full_path = f"s3://{bucket_name}{inference_models_path}".format(
        bucket_name=bucket_name, final_models_path=final_models_path
    )
    current_inference_models_full_path = \
        current_path(inference_models_full_path) + ".parquet"
    logging.info(f"current_inference_models_full_path {current_inference_models_full_path!r}")
    # Upload to the S3 bucket
    wr.s3.to_parquet(df=df_final, path=current_inference_models_full_path)

    # Load Data into Redshift
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create a new column with the current date
    df_final['snapshot_date'] = current_date

    df_final1 = df_final[['product_sku', 'pred_day_1', 'pred_day_2', 'pred_day_3',
                          'pred_day_4', 'pred_day_5', 'pred_day_6', 'pred_day_7', 'total_preds',
                          'discount_12m', 'high_price_12m', 'active_price_12m', 'model_run_id',
                          'snapshot_date']]
    engine = RedshiftSQLHook('redshift_default').get_sqlalchemy_engine()
    df_final1.to_sql(name=pricing_table,
                     con=engine,
                     if_exists='append',
                     index=False,
                     schema=pricing_schema,
                     method="multi",
                     chunksize=1000)

    # Limit data to last 20 runs
    cursor = redshift_conn("airflow_redshift_conn")
    logging.info("data_limit_to_20_runs")
    cursor.execute(data_limit_to_20_runs)
    cursor.close()

    logging.info("parquet has been written to S3 as a parquet.")
