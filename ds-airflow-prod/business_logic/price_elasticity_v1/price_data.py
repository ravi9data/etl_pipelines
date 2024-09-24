import pandas as pd

from plugins.utils.connections import redshift_conn
from plugins.utils.s3_utils import (current_path, read_file_object_s3,
                                    write_csv_file_s3)


def price_data(bucket_name, price_query_path, price_data_path):
    """
    This function is used to fetch the price data from redshift
    """
    price_query = read_file_object_s3(bucket_name, price_query_path)
    cursor = redshift_conn("airflow_redshift_conn")
    print("price_query", price_query)
    cursor.execute(price_query)
    rows = cursor.fetchall()
    print("rows[:4]", rows[:4])
    df = pd.DataFrame(rows)
    print("df********", df.head())
    columns = [
        "product_sku",
        "subcategory_name",
        "category_name",
        "brand",
        "snapshot_date",
        "year",
        "month",
        "day",
        "weekday",
        "active_price_12m",
        "high_price_12m",
        "discount_12m",
        "num_orders_m1",
        "num_orders_m3",
        "num_orders_m6",
        "num_orders_m12",
        "num_orders_m18",
        "num_orders_m24",
        "uniqviews",
        "views",
        "stock_on_hand",
        "active_subs",
        "utilisation"
    ]
    df.columns = columns
    price_data_path = f"s3://{bucket_name}{price_data_path}".format(
        bucket_name=bucket_name, price_data_path=price_data_path
    )
    current_price_data_path = current_path(price_data_path)
    write_csv_file_s3(df, current_price_data_path)
