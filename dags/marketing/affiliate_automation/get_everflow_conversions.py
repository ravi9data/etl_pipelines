import json

import awswrangler as wr
import pandas as pd
import requests
from airflow.models import Variable


def get_eflow_conversion(start_date, end_date, page_size):
    url = "https://api.eflow.team/v1/networks/reporting/conversions"
    headers = {
        'Content-Type': "application/json",
        'x-eflow-api-key': Variable.get('everflow_api_key')
    }
    page_num = 1

    payload = {
        "timezone_id": 55,
        "currency_id": "EUR",
        "from": start_date,
        "to": end_date,
        "show_events": True,
        "show_conversions": True,
        "show_only_vt": False,
        "show_only_fail_traffic": False,
        "show_only_scrub": False,
        "show_only_ct": False,
        "query": {
            "filters": [
                {"resource_type": "status", "filter_id_value": "approved"},
                {"resource_type": "label", "filter_id_value": "Affiliates"},
                {"resource_type": "label", "filter_id_value": "Influencer"}
            ],
            "search_terms": []
        }
    }

    results = requests.post(url=url, headers=headers, data=json.dumps(payload),
                            params={"page": page_num, "page_size": page_size}).json()
    rec_count = results['paging']['total_count']
    print(rec_count)

    data = results['conversions']

    while page_num * page_size < rec_count:
        page_num += 1
        results = requests.post(url=url, headers=headers, data=json.dumps(payload),
                                params={"page": page_num, "page_size": page_size}).json()
        data += results['conversions']

    # Converting to DataFrame
    df = pd.DataFrame.from_dict(data)

    if not df.empty:
        wr.s3.to_csv(
            df=df,
            path='s3://grover-eu-central-1-production-data-bi-curated/affiliate' +
            '/everflow/fetch/conversions.csv',
            sep=';',
            columns=[
                'conversion_id', 'conversion_unix_timestamp', 'sub1', 'status',
                'revenue', 'country', 'device_type', 'event', 'transaction_id',
                'click_unix_timestamp', 'sale_amount', 'coupon_code', 'order_id',
                'url', 'currency_id'
            ],
            index=False)
    else:
        print("The Conversions Dataframe is empty!")
