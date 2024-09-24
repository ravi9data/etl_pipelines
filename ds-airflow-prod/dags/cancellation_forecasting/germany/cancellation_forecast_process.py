# Import libraries

import logging
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from dags.cancellation_forecasting.germany import create_data, model

logger = logging.getLogger(__name__)


def run_cancellation_forecasts(
    start_date="2019-01-01",
    train_test_cutoff_date=datetime.today().date().replace(day=1),
    end_date=datetime.today().date().replace(day=1) + relativedelta(months=11),
    month_cutoff=datetime.today().date().replace(day=1),
    subcategory_data=None,
    final_models_table=None,
    use_targets=True,
):
    logging.info("Query and create the dataset")

    cohort_data, future_data, avg_asv = create_data.create_dataframe(
        start_date=start_date,
        end_date=end_date,
        train_test_cutoff_date=train_test_cutoff_date,
        subcategory_data=subcategory_data,
        month_cutoff=month_cutoff,
        use_targets=use_targets,
    )

    logging.info("Train the model and get the forecasts")
    final_results = pd.DataFrame()

    for _, row in final_models_table.iterrows():
        fitted_model, ts_forecast, train_data = model.model_train(
            cohort_data, row, train_test_cutoff_date
        )

        model_predictions = model.model_forecast(
            row,
            future_data,
            ts_forecast,
            train_data,
            fitted_model,
            train_test_cutoff_date,
            avg_asv,
        )

        final_results = pd.concat(
            [
                final_results,
                model_predictions,
            ]
        )

    final_results["country"] = 'Germany'
    final_results["store"] = "Grover Germany"
    final_results["customer_type"] = "normal_customer"
    final_results["update_date"] = datetime.today()

    return final_results
