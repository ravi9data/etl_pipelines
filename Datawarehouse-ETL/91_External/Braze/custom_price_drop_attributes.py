import csv
import sys
import json
import boto3 
import requests
import concurrent.futures
import time
import datetime

S3_BUCKET_NAME = 'redshift-matillion-backup'
S3_PREFIX = 'braze_price_drop'

# Parameters for connecting to S3
s3 = boto3.resource('s3')
bucket = s3.Bucket(S3_BUCKET_NAME)

# Parameters for accessing Braze API, token is available in Matillion and 1password
url = "https://rest.fra-01.braze.eu/users/track"
header = {'Authorization': "Bearer " + token, 'Content-Type':'application/json'}

# Builds list of columns, store as null otherwise
column_names = []
column_names_set = False


def set_columns(from_row):
    for column in from_row:
        column_names.append(column)


def check_rate_limit(response):
    """
    Function to check current response limit. If it reaches 20, there will be an error
    """
    return int(response.headers["X-RateLimit-Remaining"]) <= 20


# JSON files are normally string, but Braze requires timestamp column to be sent in the timestamp format.
# So this ensures the proper metadata format
row_types = {
    'external_id': str,
    # cart attributes
    'mtl_cart_price_drop_names': str,
    'mtl_cart_price_drop_full_prices': str,
    'mtl_cart_price_drop_sale_prices': str,
    'mtl_cart_price_drop_images': str,
    'mtl_cart_price_drop_links': str,

    # wishlist
    'mtl_wishlist_price_drop_names': str,
    'mtl_wishlist_price_drop_full_prices': str,
    'mtl_wishlist_price_drop_sale_prices': str,
    'mtl_wishlist_price_drop_images': str,
    'mtl_wishlist_price_drop_links': str,
    
    'updated_at': str,
    None: str # File always has empty space at the end
} 

# Lambda function to process each column (if datetime format is needed)
date_handler = lambda obj: (
    obj.isoformat()
    if isinstance(obj, (datetime.datetime, datetime.date))
    else None
)


def send_data(record):
    """
    # Function to send data to Braze using Requests module.
    :param record: dict
    :return: int, the status code of the sending result
    """
    try:
        response = requests.post(url, headers=header, data=json.dumps({"attributes": [record]}, default=date_handler))
        if response.status_code not in (200, 201):
            print('HTTP Error:', response.json())
        if check_rate_limit(response):
            print('X-Rate Limit reached, sleep for 10 seconds')
            time.sleep(10)
        return response.status_code
    except Exception as error:
        print(error)
        return 999


def handle_csv(input_csv, key, separator=';'):
    if input_csv:
        splitted = input_csv.split(separator)
        return [f'{val}_{int(idx)+1}' if '_prices' in key else val for idx, val in enumerate(splitted)]


def _adding_key_update_existing_only(record):
    """
    Function to add key "_update_existing_only" and value True at the second postion 
    of the input record 
    :param record: dict
    :return: dict, with same keys as input + key "_update_existing_only" and value True at the second postion
    """    
    new_record=record.__class__()
    for k,v in record.items():
        new_record[k]=v
        if k=='external_id':
            new_record['_update_existing_only']=True
    return new_record


def transform_row(record):
    _row_transformed = {k: row_types[k](v) for k, v in record.items()}
    for k in set(_row_transformed.keys()) - {'external_id'}:
        _row_transformed[k] = handle_csv(_row_transformed[k],key=k)
    _row_transformed['mtl_price_drop_updated_at'] = datetime.datetime.utcnow().isoformat()
    _row_transformed=_adding_key_update_existing_only(_row_transformed)
    return _row_transformed


# Executes functions using multiprocessing module. Each row is read line-by-line, and the
# datatype is fixed accordingly using the row_types dictionary. After each record is properly set to
# the expected format, the send_data function is run.

print('Starting to send....')
results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []
    for s3_object in bucket.objects.filter(Prefix=S3_PREFIX):
        str_object = s3_object.get()["Body"].read()
        str_object = str_object.decode()
        csv_data = csv.DictReader(str_object.split('\n'), skipinitialspace=True, escapechar='\\', quotechar='"')
        for row in csv_data:
            if not column_names_set:
                set_columns(row)
                column_names_set = True
            row_converted = transform_row(row)
            futures.append(executor.submit(send_data, row_converted))

    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
print('Finish to send!')
total_sent = len(results)
failures = len(list(i for i in results if i not in (200, 201)))
failure_rate = (failures/total_sent) * 100

print(f'Records sent: {total_sent} with {failures} failures, failure rate: {failure_rate} %')

if failure_rate > 1:  # we have more than 1% of failures
    raise Exception(f'Failure rate over threshold, records sent: {total_sent} with {failures} failures')
