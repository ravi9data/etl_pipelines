# Import libraries
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def model_train(cohort_data, row, train_test_cutoff_date):
    subcat = row['subcategory_name']
    feats_names = row['features']
    model = row['model']

    # Filter data by date and subcategories
    x_train = cohort_data[
        (cohort_data.cancellation_date < train_test_cutoff_date) &
        (cohort_data.subcategory_name.isin(subcat))
        ]
    train_data = x_train[['start_date', 'cancellation_date', 'y', 'subcategory_name']]
    x_train = x_train.rename(columns={'cancellation_date': 'ds'})

    x_train = x_train[feats_names]

    y_train = x_train['y']
    X_train = x_train.drop(['y'], axis=1)

    # Fit the model
    model.fit(X_train, y_train)

    return model, train_data


def model_forecast(row, future_data, train_data, model, train_test_cutoff_date, avg_asv):
    acquisitions = future_data.groupby(['start_date', 'subcategory_name'])[
        ['cohort_asv', 'n_cohort_subscriptions']].max().reset_index()

    N_MONTHS = 12
    final_result_all = pd.DataFrame()
    subcat = row['subcategory_name']
    feats_names = row['features']

    # Run predictions for each subcategory, and append the results
    # to the final output (with all subcategories)
    for s in subcat:
        x_test = future_data[(future_data.subcategory_name == s)]

        test_data = x_test.copy()

        x_test = x_test.rename(columns={'cancellation_date': 'ds'})

        x_test = x_test[feats_names]

        # y_test = x_test['y']
        X_test = x_test.drop(['y'], axis=1)

        logging.info("Predictions with the fitted model")
        preds = model.predict(X_test)
        preds = pd.Series(preds)

        final_preds = pd.concat([
            test_data[['start_date', 'cancellation_date', 'y']].reset_index(drop=True),
            pd.Series(preds).rename('yhat')], 1)

        final_preds['yhat'] = final_preds['yhat'].round()

        subcat_pred = pd.concat([
            final_preds,
            test_data[['subcategory_name']].reset_index(drop=True)], 1)

        logging.info("Create a matrix with acquisition cohorts and cancellation cohorts")
        test_matrix = subcat_pred.pivot(
            index='start_date',
            columns='cancellation_date',
            values='yhat'
        )

        # There could be some values predicted as negative. Let's set them to 0 instead.
        test_matrix[test_matrix < 0] = 0

        logging.info("Create a matrix with acquisition cohorts and "
                     "cancellation cohorts for the train data")
        train_matrix = train_data[train_data.subcategory_name == s].pivot(
            index='start_date',
            columns='cancellation_date',
            values='y'
        )

        logging.info("Combine train and test matrices")
        final_matrix_preds = train_matrix.merge(test_matrix, on='start_date', how='outer')

        logging.info("Deduce ASV from the initial total ASVs and the cancellation rates")
        acquisitions_subcat = acquisitions[acquisitions.subcategory_name == s]
        cancellations_rate_matrix = (final_matrix_preds.T / acquisitions_subcat.set_index(
            'start_date').n_cohort_subscriptions).round(3).T
        final_matrix_preds_asv = (acquisitions_subcat.set_index(
            'start_date').cohort_asv * cancellations_rate_matrix.T).T
        final_matrix_preds_asv = final_matrix_preds_asv.replace(np.inf, np.nan)
        # for future values for which we don't know the acquired subs or asv:
        avg_asv_subcat = avg_asv[avg_asv.subcategory_name == s].iloc[-1, -1]
        final_matrix_preds_asv.iloc[-N_MONTHS:, -N_MONTHS:] = final_matrix_preds_asv.iloc[
                                                              -N_MONTHS:, -N_MONTHS:].fillna(
            final_matrix_preds.iloc[-N_MONTHS:, -N_MONTHS:] * avg_asv_subcat
        )

        logging.info("Combine subcategory forecasts to other subcategories")
        final_result_all = pd.concat([
            final_result_all,
            pd.concat([
                pd.DataFrame(final_matrix_preds.sum().loc[train_test_cutoff_date:]).rename(
                    columns={0: 'cancelled_subscriptions'}).reset_index(),
                pd.DataFrame(final_matrix_preds_asv.sum().loc[train_test_cutoff_date:]).rename(
                    columns={0: 'cancelled_asv'}).reset_index(drop=True).round(),
                pd.Series([s] * N_MONTHS).rename('subcategory'),
            ], axis=1)
        ], axis=0)

    return final_result_all
