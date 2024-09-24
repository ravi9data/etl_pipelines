from datetime import datetime, timedelta

import boto3
import requests
from airflow.models import Variable


def fetch_data():
    url = "https://export.daisycon.com/advertisers/7226/transactions/csv"
    params = {"start_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
              "end_date": datetime.now().strftime("%Y-%m-%d"),
              "currency_code": "EUR",
              "delimiter": "semicolon",
              "enclosure": "doublequote",
              "status[]": "open",
              "username": "bi@grover.com",
              "token": Variable.get('daisycon_token')
              }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        resp = response.text
        filename = 'daisycon.csv'
        s3 = boto3.resource("s3")
        s3.Object("grover-eu-central-1-production-data-bi-curated",
                  "affiliate/daisycon/fetch/" + filename, ).put(Body=resp)
    else:
        raise
