import pandas as pd
from sklearn.ensemble import RandomForestClassifier


def train_data():
    # new_training_data_20201118.csv
    x_train = pd.read_csv(
        "./dags/labels/data/x_train_data_labels_20201119.csv"
    ).set_index("customer_id")
    y_train = (
        pd.read_csv("./dags/labels/data/y_train_data_labels_20201119.csv")
        .set_index("customer_id")
        .iloc[:, 0]
    )

    return x_train, y_train


def model_fit(x_test):
    x_train, y_train = train_data()

    classifier = RandomForestClassifier(
        bootstrap=True,
        ccp_alpha=0.0,
        class_weight="balanced",
        criterion="entropy",
        max_depth=7,
        max_features=0.65,
        max_leaf_nodes=34,
        max_samples=0.95,
        min_impurity_decrease=0.0,
        min_samples_leaf=2,
        min_samples_split=2,
        min_weight_fraction_leaf=0.0,
        n_estimators=300,
        n_jobs=12,
        oob_score=False,
        random_state=42,
        verbose=0,
        warm_start=False,
    )

    classifier.fit(x_train, y_train)
    pred = classifier.predict(x_test)
    pred_prob = classifier.predict_proba(x_test)

    return pred, pred_prob


def final_data(x_test, pred, pred_prob):
    prediction_data = pd.concat(
        [
            pd.Series(x_test.index.tolist()).rename("customer_id"),
            pd.Series(pred).rename("label"),
            pd.DataFrame(pred_prob)
            .rename(
                columns={
                    0: "prob_credit_default",
                    1: "prob_fraud",
                    2: "prob_good",
                    3: "prob_recovered_bad",
                    4: "prob_recovered_good",
                    5: "prob_uncertain",
                }
            )
            .round(4),
        ],
        1,
    ).set_index("customer_id")

    return prediction_data
