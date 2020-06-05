import random
import json 
import datetime
import time

import boto3

client = boto3.client('kinesis')

def getReferrer():
    x = random.randint(1,5)
    x = x*50 
    y = x+30 
    data = {}
    data['USER_ID'] = random.randint(x,y)
    data['DEVICE_ID'] = random.choice(['mobile','computer', 'tablet', 'mobile','computer'])
    data['CLIENT_EVENT'] = random.choice(['beer_vitrine_nav','beer_checkout','beer_product_detail',
    'beer_products','beer_selection','beer_cart'])
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['CLIENT_TIMESTAMP'] = str_now
    return data

def lambda_handler(event, context):
    
        
        while True:
            client.put_record(
                StreamName='streaming_data_test',
                Data=json.dumps(getReferrer()),
                PartitionKey='partitionkey')
