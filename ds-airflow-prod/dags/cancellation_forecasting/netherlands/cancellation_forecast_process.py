# Import libraries

import logging
from datetime import datetime

import pandas as pd

# For the model, we can use the same code as the one for germany_b2b
from dags.cancellation_forecasting.germany_B2B import model
from dags.cancellation_forecasting.netherlands import create_data

logger = logging.getLogger(__name__)


def run_cancellation_forecasts(
        start_date,
        train_test_cutoff_date,
        end_date,
        month_cutoff,
        subcategory_data,
        final_models_table,
        use_targets,
        ):

    logging.info('Query and create the dataset')
    cohort_data, future_data, avg_asv = create_data.create_dataframe(
        start_date=start_date,
        end_date=end_date,
        train_test_cutoff_date=train_test_cutoff_date,
        subcategory_data=subcategory_data,
        month_cutoff=month_cutoff,
        use_targets=use_targets,
    )

    logging.info('Train the model and get the forecasts')
    final_results = pd.DataFrame()

    for _, row in final_models_table.iloc[1:, :].iterrows():
        fitted_model, train_data = model.model_train(cohort_data, row, train_test_cutoff_date)

        model_predictions = model.model_forecast(row, future_data, train_data, fitted_model,
                                                 train_test_cutoff_date, avg_asv)

        final_results = pd.concat([
            final_results,
            model_predictions,
        ])

    logging.info('Combine ML and Prophet forecasts')
    prophet_final_results = future_data.groupby(['cancellation_date', 'subcategory_name'])[
        ['yhat']].min().reset_index()
    prophet_final_results = prophet_final_results.merge(
        avg_asv.groupby(['subcategory_name']
                        )[['subscription_value']].last(),
        on='subcategory_name',
        how='left')
    prophet_final_results['yhat'] = prophet_final_results['yhat'].round()
    prophet_final_results['cancelled_asv'] = prophet_final_results['yhat'] * prophet_final_results[
        'subscription_value']
    prophet_final_results = prophet_final_results.rename(
        columns={
            'yhat': 'cancelled_subscriptions',
            'subcategory_name': 'subcategory'
        }
    )

    final_results = pd.concat([
        final_results,
        prophet_final_results[
            prophet_final_results.subcategory.isin(final_models_table['subcategory_name'].iloc[0])][
            ['cancellation_date', 'cancelled_subscriptions', 'cancelled_asv', 'subcategory']
        ]
    ])

    final_results.loc[final_results.cancelled_subscriptions <= 0, 'cancelled_subscriptions'] = 0
    final_results.loc[final_results.cancelled_asv <= 0, 'cancelled_asv'] = 0
    final_results.loc[final_results.cancelled_subscriptions == 0, 'cancelled_asv'] = 0

    final_results[['cancelled_asv', 'cancelled_subscriptions']] = final_results[
        ['cancelled_asv', 'cancelled_subscriptions']
    ].round()
    final_results['country'] = 'Netherlands'
    final_results['store'] = 'Grover International'
    final_results['customer_type'] = 'normal_customer'
    final_results['update_date'] = datetime.today()

    return final_results
