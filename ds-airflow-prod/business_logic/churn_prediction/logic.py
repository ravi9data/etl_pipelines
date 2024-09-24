import logging
import pickle

import airflow.providers.amazon.aws.hooks.redshift_sql as rd
import pandas as pd

from plugins.utils.s3_utils import read_file_object_s3
from plugins.utils.sql import read_sql_file

from .variables import BUCKET, MODEL_PATH, SCHEMA, TABLE_NAME


def get_subscription_features(final_features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Import subscription info
    rs = rd.RedshiftSQLHook(redshift_conn_id='redshift')
    redshift_engine = rs.get_sqlalchemy_engine()

    subscriptions_sql = read_sql_file("./dags/churn_prediction/sql/Test_data.sql")

    logging.info(subscriptions_sql)

    subscriptions_ending_soon = pd.read_sql(
        subscriptions_sql,
        redshift_engine,
        parse_dates=['start_date', 'cancellation_date']
    )
    # Preprocess the dataset
    subscriptions_ending_soon.columns = subscriptions_ending_soon.columns.str.strip()
    subscriptions_ending_soon = subscriptions_ending_soon.dropna(subset=['category_name'])
    subscriptions_ending_soon['has_other_rental'] = (
        subscriptions_ending_soon['has_other_rental']
        .map({'Yes': True, 'No': False})
    )

    # Perform one-hot encoding on categorical features
    subscriptions_ending_soon_enc = pd.get_dummies(
        subscriptions_ending_soon,
        columns=['age', 'category_name'],
        drop_first=True
    )

    # Drop unnecessary columns
    columns_to_drop = [
        'subscription_id',
        'product_name',
        'subcategory_name',
        'brand',
        'start_date',
        'cancellation_date'
    ]
    subscriptions_ending_soon_enc.drop(columns=columns_to_drop, axis=1, inplace=True)
    # Ensure matching column names for one-hot encoded columns
    missing_cols = list(set(final_features) - set(subscriptions_ending_soon_enc.columns))
    subscriptions_ending_soon_enc[missing_cols] = 0  # Add missing columns with zeros

    # Extract the features for prediction from the new dataset
    feature_vector = subscriptions_ending_soon_enc.drop(
        ['customer_id', 'product_sku'],
        axis=1,
        errors='ignore'
    )

    # Ensure the order of columns in new_X matches the order during training
    feature_vector = feature_vector[final_features]
    return subscriptions_ending_soon, feature_vector


def generate_predictions() -> None:
    # Load the trained model
    model_data = read_file_object_s3(
        bucket_name=BUCKET,
        query_path=MODEL_PATH,
        decode_type='binary'
    )
    loaded_model = pickle.loads(model_data)
    # Load the new dataset
    new_df, new_X = get_subscription_features(final_features=loaded_model.feature_names_in_)

    # Make probability predictions on the new dataset
    new_y_prob = loaded_model.predict_proba(new_X)[:, 1]

    # Define a threshold for class "1" probabilities (if needed)
    new_threshold = 0.5

    # Use the new threshold to classify samples into class "1" or class "0"
    new_y_pred_adjusted = (new_y_prob >= new_threshold).astype(int)

    # Add predictions to the dataframe
    new_df['predictions'] = new_y_pred_adjusted

    # Filter the false customers
    false_customers = new_df[new_df.predictions == 0]

    # Drop duplicate rows based on 'customer_id' from false_customers
    false_customers = false_customers.drop_duplicates(subset='customer_id')

    rs = rd.RedshiftSQLHook(redshift_conn_id="redshift")
    redshift_engine = rs.get_sqlalchemy_engine()

    logging.info("Write the results to DB")
    false_customers.to_sql(
        TABLE_NAME,
        con=redshift_engine,
        schema=SCHEMA,
        if_exists="append",
        method="multi",
        index=False,
    )
