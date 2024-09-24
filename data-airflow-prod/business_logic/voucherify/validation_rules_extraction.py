import ast
import json

import awswrangler as wr
import pandas as pd

from plugins.voucherify_utils import get_iso_date_range, write_csv_file_s3


def validation_rules_extraction_to_s3(ti, **kwargs):
    # Pull the full S3 path from XCom
    s3_location = ti.xcom_pull(task_ids='get_validation_rules',
                               key='s3_file_path_validation_rules')
    data = wr.s3.read_csv(path=[s3_location])
    data['rules'] = data['rules'].apply(ast.literal_eval)

    rules_list = []
    for i in range(0, data.shape[0]):
        for key, value in data.iloc[i]['rules'].items():
            if key == 'logic':
                continue
            else:
                rule = {
                    'val_rule_id': data.iloc[i]['id'],
                    'val_rule_name': data.iloc[i]['name'],
                    'rules': data.iloc[i]['rules'],
                    'created_at': data.iloc[i]['created_at'],
                    'type': data.iloc[i]['type'],
                    'context_type': data.iloc[i]['context_type'],
                    'assignments_count': data.iloc[i]['assignments_count'],
                    'object': data.iloc[i]['object'],
                    'applicable_to': data.iloc[i]['applicable_to'],
                    'rule_id': key,
                    'name': value.get('name'),
                    'property': value.get('property'),
                    'conditions': value.get('conditions')}
                rules_list.append(rule)
    df = pd.DataFrame(rules_list)

    df['rules'] = list(map(lambda x: json.dumps(x), df['rules']))
    df['applicable_to'] = list(map(lambda x: json.dumps(x), df['applicable_to']))
    df['conditions'] = list(map(lambda x: json.dumps(x), df['conditions']))

    # Dates
    PAST_DAY_ISO_FORMAT, TODAY_ISO_FORMAT = get_iso_date_range(n_days_ago=1)

    BUCKET_NAME = "grover-eu-central-1-production-data-raw"
    ENTITY = 'validation_rules_extraction'
    S3_PREFIX = "voucherify"

    s3_path = write_csv_file_s3(dataframe=df,
                                s3_bucket=BUCKET_NAME,
                                s3_prefix=S3_PREFIX,
                                entity=ENTITY,
                                date=TODAY_ISO_FORMAT,
                                filename=ENTITY)
    ti.xcom_push(key=f's3_file_path_{ENTITY}',
                 value=s3_path)
