import numpy as np

from business_logic.customer_labels4.constants import (FILL_NA_0, FILL_NA_9,
                                                       LABEL_TRANSACTIONS_DICT,
                                                       RULES, RULESET)


def process_features(data):

    data.loc[:,FILL_NA_0] = data.loc[:,FILL_NA_0].fillna(0)

    data.loc[:,FILL_NA_9] = data.loc[:,FILL_NA_9].fillna(-9)

    # data = data.(np.nan)

    data.loc[:,'failed_transaction_reasons_new'] = np.nan

    for key,value in LABEL_TRANSACTIONS_DICT.items():

        filter_label = [key in x for x in data['failed_transaction_reasons'].astype('str')]
        data.loc[filter_label,'failed_transaction_reasons_new'] = value

    return data


def create_labels(data, ruleset_version):

    # Create labels
    data.loc[:,'label'] = np.nan

    data.loc[:,'rule'] = np.nan

    rules = dict(
        zip(
            RULESET[ruleset_version],
            [RULES[k] for k in RULESET[ruleset_version]]
        )
    )

    for rule_name, rule in rules.items():
        data_ = data.query(rule[0])

        data.loc[data_.index, 'label'] = rule[1]
        data.loc[data_.index, 'rule'] = rule_name

    return data
