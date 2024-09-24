import csv
import sys
import json
import boto3 
import requests
import concurrent.futures
import time

# Parameters for connecting to S3
s3 = boto3.resource('s3')
bucket = s3.Bucket('redshift-matillion-backup')

# Parameters for accessing Braze API, token is available in Matillion and 1password
url = "https://rest.fra-01.braze.eu/users/track"
header = {'Authorization': "Bearer " + token + api_key, 'Content-Type':'application/json'}

# Builds list of columns, store as null otherwise
column_names = []
column_names_set = False

def set_columns(from_row):
    for column in from_row:
        column_names.append(column)

# Function to check current response limit. If it reaches 20, there will be an error
def check_rate_limit(response):
    return int(response.headers["X-RateLimit-Remaining"]) <= 20


# JSON files are normally string, but Braze requires timestamp column to be sent in the
# timestamp format. So this ensures the proper metadata format
row_types={
'external_id': str,
'almost_ending_subscription_names': str,
'almost_ending_subscription_ids': str,
'almost_ending_subscription_periods':str,
'same_day_ending_subs_count': str,
'closest_minimum_cancellation_date': str,
'active_subscriptions': str,
None: str}

# Lambda function to process each column
date_handler = lambda obj: (
    obj.isoformat()
    if isinstance(obj, (datetime.datetime, datetime.date))
    else None
)

# Function to send data to Braze using Requests module.
def send_data(row):
    row=[row]
    #data=json.dumps({
    #"attributes": row
    #}, default=date_handler)
    #print(data)
    response = requests.post(url, headers=header, data=json.dumps({"attributes": row}, default=date_handler))
    if response.status_code not in (200, 201):
        print('HTTP Error:', response.json())
        sys.exit()
    #print('HTTP request code:',response.json())
    if check_rate_limit(response):
        print('X-Rate Limit reached, sleep for 10 seconds')
        time.sleep(10)

# Executes functions using multiprocessing module. Each row is read line-by-line, and the
# datatype is fixed accordingly using the row_types dictionary. After each record is properly set to
# the expected format, the send_data function is run.
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []
    for object in bucket.objects.filter(Prefix='braze_subs'):
        str_object = object.get()["Body"].read()
        str_object = str_object.decode()
        csv_data = csv.DictReader(str_object.split('\n'), skipinitialspace=True, escapechar='\\', quotechar='"')
        for row in csv_data:
            if not column_names_set:
                set_columns(row)
                column_names_set = True
            row_converted = {k: row_types[k](v) for k, v in row.items()}
            #print(row_converted)
            futures.append(executor.submit(send_data, row_converted))

    for future in concurrent.futures.as_completed(futures):
        pass

print('Script completed')
