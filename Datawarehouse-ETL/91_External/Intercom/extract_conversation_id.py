#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: yuvaraj

Incrementally retrieve all conversation IDs with timestamps from Intercom conversations list.
"""
import csv
import boto3
import requests
import time
from datetime import datetime
from concurrent import futures
import os, sys

os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))
s3 = boto3.resource("s3")

timestamp=datetime.now()
print('Current timestamp:',timestamp)


c_url = 'https://api.intercom.io/conversations?per_page=50'
headers = {
    'Authorization': "Bearer " + token,
    'Accept': 'application/json',
}



# Change here for desired start and end pages. Otherwise its automatic from the Matillion job parameter
start_page=1
#end_page=33010


page_list = list(range(int(start_page),int(end_page)+1))


def check_rate_limit(response):
    return int(response.headers["X-RateLimit-Remaining"]) <= 20

    
def convert_unixts(ts):
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def extract_all_ids(request):
    conversations=request['conversations']
    conv_id = {x['id']: x for x in conversations}
    conv_created_at = {x['created_at']: x for x in conversations}
    conv_updated_at = {x['updated_at']: x for x in conversations}
    #id_list = list(zip(list(conv_id.keys()),list(conv_created_at.keys()),list(conv_updated_at.keys())))
    id_list = list(zip(list(conv_id.keys()),
                   [convert_unixts(x) for x in list(conv_created_at.keys())],
                   [convert_unixts(x) for x in list(conv_updated_at.keys())]))
    return id_list


def process_conv_id(page_range):
    
    p=page_range
    #p = page_range.replace(',', '')
    #p = p.strip()
    #print(p)
        
    params = (
        ('order', 'asc'),
        ('sort', 'created_at'),
        ('page', p),   
        )
        
    #filename='intercom_'+str(p)+'.csv'
    filename = f'intercom_{p}.csv'
    #print(filename)
    
    with open(filename, 'w',encoding='utf-8') as csvfile:
       
        writer = csv.writer(csvfile)
        response = requests.get(c_url, headers=headers, params=params)
        r = response.json()
        id_list = extract_all_ids(r)
    
        for item in id_list:
            writer.writerow(item)
            
            
    with open(filename, 'r') as f:
        s3object.put(Body=bytearray(f.read(), "utf-8"))
            
            
    if check_rate_limit(response):
        #print('X-ratelimit reached, waiting for 10 seconds')
        time.sleep(10)
            


def execute_all(page_list):
    with futures.ThreadPoolExecutor(max_workers=200) as executor:
        resp_err, resp_ok = 0, 0
        futs = {executor.submit(process_conv_id, page_range): page_range for page_range in page_list}

        for future in futures.as_completed(futs):
            conv_done = futs[future]
            try:
                _ = future.result()
            except Exception as ex:
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

# A list of pages
execute_all(page_list)
print('Done')
