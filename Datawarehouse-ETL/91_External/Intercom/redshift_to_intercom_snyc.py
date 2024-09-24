import requests
import json
import boto3 
import csv
import concurrent.futures
import time

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

url = 'https://api.intercom.io/users'

headers = {'Authorization':"Bearer " + token,'Content-Type':'application/json','Accept':'application/json'}

s3 = boto3.resource('s3')

column_names = []
column_names_set = False

def check_rate_limit(response):
  return response.headers["X-RateLimit-Remaining"] <= 20

def build_attributes(from_row):
  attributes = {}
  for index, column_value in enumerate(from_row):
    if column_names[index] != 'user_id':
      attributes[column_names[index]] = column_value
  attributes['redshift_synced_at'] = str(int(time.time()))
  attributes['sync_test_number'] = 9
  return attributes

def process_row(row):
  data = build_attributes(row)
  # print(json.dumps({
  #   "user_id": row[0],
  #   "data": data
  # }))
  update_request = requests.post(url, headers=headers, data=json.dumps({
		"user_id": row[0],
		"custom_attributes": json.dumps(data)
	}))
  if update_request.status_code != 200:
    print(update_request.content)
  if check_rate_limit(update_request):
    time.sleep(10)

def set_columns(from_row):
  for column in from_row:
    column_names.append(column)

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
  futures = []
  for object in bucket.objects.filter(Prefix='userInfo'):
    string_object = object.get()["Body"].read()
    csv_reader = csv.reader(string_object.split('\n'), delimiter=',')
    for row in csv_reader:
      if not column_names_set:
        set_columns(row)
        column_names_set = True
      futures.append(executor.submit(process_row, row))
  total = len(futures)
  for future in concurrent.futures.as_completed(futures):
    pass

print('done')

