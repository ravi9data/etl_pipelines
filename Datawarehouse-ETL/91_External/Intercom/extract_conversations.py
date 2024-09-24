#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: yuvaraj

Incrementally retrieve all conversation using conversation_id as the search parameter.
"""

import asyncio
import concurrent
import json
import os
import sys
import time
import boto3
import requests
from datetime import datetime
from concurrent import futures

os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))
s3 = boto3.resource("s3")

THREAD_COUNT = 10


headers = {'Authorization': "Bearer " + token,
                            'Accept': 'application/json', }


abc = str(conv_id)
abc = abc.replace("None", "\'none\'")
abc = abc.replace("u'", "'")
abc = abc.replace("[", " ")
abc = abc.replace("]", " ")
conv_id_list = abc.split("), (")


def check_rate_limit(response):
    return int(response.headers["X-RateLimit-Remaining"]) <= 20


def process_conv_id(conv_id):

    id = conv_id.replace('(', '')
    id = id.replace(')', '')
    id = id.replace("'", '')
    id = id.replace(',', '')
    id = id.strip()

    #print('id',id)


    c_url = 'https://api.intercom.io/conversations/' + str(id)

    payload = {}
    response = requests.get(url=c_url, headers=headers, data=payload)
    response_data = response.json()

    filename = f'intercom1_{id}.json'

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(response_data, f)

    with open(filename, 'r') as f:
        s3object.put(Body=bytearray(f.read(), "utf-8"))

    if check_rate_limit(response):
        time.sleep(10)


def execute_all(conv_id_list):
    with futures.ThreadPoolExecutor(max_workers=200) as executor:
        resp_err, resp_ok = 0, 0
        futs = {executor.submit(process_conv_id, conv_id): conv_id for conv_id in conv_id_list}
        
        for future in futures.as_completed(futs):
            conv_done = futs[future]
            try:
                _ = future.result()
            except Exception as ex:
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

    print('Number of errors:',resp_err)

# A list of conversation ids => [conv_id1, conv_id2, ....]
execute_all(conv_id_list)

print('Done')

