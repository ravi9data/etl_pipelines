import csv
import sys
import json
import boto3 
import requests
import concurrent.futures
import time
import datetime

S3_BUCKET_NAME = 'redshift-matillion-backup'
S3_PREFIX = 'black_friday_braze_price_drop'
SENT_RECORDS=0

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

# Function to check current response limit. If it reaches 20, there will be an error
def check_rate_limit(response):
    return int(response.headers["X-RateLimit-Remaining"]) <= 20


# JSON files are normally string, but Braze requires timestamp column to be sent in the timestamp format. So this ensures the proper metadata format
row_types = {
    'external_id': str,
    # cart attributes
    'last_mtl_wishlist_price_drop_names': str,
    'prev_mtl_wishlist_price_drop_names': str,
    'last_mtl_wishlist_price_drop_full_prices': str,
    'prev_mtl_wishlist_price_drop_full_prices': str,
    'last_mtl_wishlist_price_drop_sale_prices': str,
    'prev_mtl_wishlist_price_drop_sale_prices': str,
    'last_mtl_wishlist_price_drop_links': str,
    'prev_mtl_wishlist_price_drop_links': str,

    'updated_at': str,
    None: str # File always has empty space at the end
} 

# Lambda function to process each column (if datetime format is needed)
date_handler = lambda obj: (
    obj.isoformat()
    if isinstance(obj, (datetime.datetime, datetime.date))
    else None
)


# Function to send data to Braze using Requests module.
def send_data(row):
    response = requests.post(url, headers=header, data=json.dumps({"attributes": [row]}, default=date_handler))
    if response.status_code not in (200, 201):
        print('HTTP Error:', response.json())
        sys.exit()
    #print('HTTP request code:',response.json())
    if check_rate_limit(response):
        print('X-Rate Limit reached, sleep for 10 seconds')
        time.sleep(10)
    return response.status_code

def handle_csv(input_csv, key, separator=';'):
    if input_csv:
        splitted = input_csv.split(separator)
        return [f'{val}_{int(idx)+1}' if '_prices' in key else val for idx, val in enumerate(splitted)]

def _adding_key_update_existing_only(record):
    new_record=record.__class__()
    for k,v in record.items():
        new_record[k]=v
        if k=='external_id':
            new_record['_update_existing_only']=True
    return new_record

def _wrapp_in_attributes(record):
    record_with_attributes=record.__class__()
    record_with_attributes['attributes']=record
    
    return record_with_attributes

def transform_row(record):
    _row_transformed = {k: row_types[k](v) for k, v in record.items()}
    for k in set(_row_transformed.keys()) - {'external_id'}:
        _row_transformed[k] = handle_csv(_row_transformed[k],key=k)
    _row_transformed['mtl_price_drop_updated_at'] = datetime.datetime.utcnow().isoformat()
    _row_transformed=_adding_key_update_existing_only(_row_transformed)
    # _row_transformed=_wrapp_in_attributes(_row_transformed)
    return _row_transformed

def compare_sets(sets):
    last_set = set(sets[0])
    prev_set = set(sets[1])
    diff = prev_set.symmetric_difference(last_set)
    
    return list(diff)

def locate_index_in_last_set(last_set,diff_elements):
    
    list_of_index=[index for element in diff_elements for index,value in enumerate(last_set) if value == element]
    return {'mtl_price_drop_wishlist_delta_indexes' : sorted(list_of_index),
           'mtl_price_drop_wishlist_delta_indexes_update_at': str(datetime.datetime.now())}

# Executes functions using multiprocessing module. Each row is read line-by-line, and the
# datatype is fixed accordingly using the row_types dictionary. After each record is properly set to
# the expected format, the send_data function is run.
print('Starting to send....')
results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = []
    for object in bucket.objects.filter(Prefix=S3_PREFIX):
        str_object = object.get()["Body"].read()
        str_object = str_object.decode()
        csv_data = csv.DictReader(str_object.split('\n'), skipinitialspace=True, escapechar='\\', quotechar='"')
        for row in csv_data:
            braze_record={}
            if not column_names_set:
                set_columns(row)
                column_names_set = True
            row_converted = transform_row(row)
            # if last whislist is empty we dont send anything
            if row_converted['last_mtl_wishlist_price_drop_names'] is not None:
                # if prev whislist is empty we populate it with an empty list to allow set comparasion
                if row_converted['prev_mtl_wishlist_price_drop_names'] is None:
                    row_converted['prev_mtl_wishlist_price_drop_names']=[]
                diff = compare_sets((row_converted['last_mtl_wishlist_price_drop_names'],row_converted['prev_mtl_wishlist_price_drop_names']))
                if len(diff)>0:
                    braze_record.update(locate_index_in_last_set(row_converted['last_mtl_wishlist_price_drop_names'],diff))
                    braze_record['external_id'] = row_converted['external_id']
                    braze_record['mtl_wishlist_price_drop_links'] = row_converted['last_mtl_wishlist_price_drop_links']
                    #sending result           
                    futures.append(executor.submit(send_data, braze_record))

    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())

print('Finish to send!')
total_sent = len(results)
failures = len(list(i for i in results if i not in (200, 201)))

print(f'Records sent: {total_sent} with {failures} failures')
