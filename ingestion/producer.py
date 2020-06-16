import random
import json 
import datetime
import time

import boto3

client = boto3.client('kinesis')
    
def getReferrer():
    
    # create data directory
    data = {}
    
    # session id
    x = random.randint(1,6)
    x = x*50 
    y = x+30 
    data['SESSION_ID'] = random.randint(x,y)
    
    # item id
    data['ITEM_ID'] = random.randint(x,y)
    
    # catogory
    real_category = random.randint(1,12)
    brand = random.randint(10**8,10**10-1) # 8-10 digits
    if x == 50:
        data['CATEGORY'] = 'MISSING_VALUE'
    elif x == 100:
        data['CATEGORY'] = 'SPEICAL_OFFER'
    elif x == 150:
        data['CATEGORY'] = 'FURNITURE'
    elif x == 200:
        data['CATEGORY'] = 'ELECTRONICS'
    elif x == 250:
        data['CATEGORY'] = 'SPORTS'
    else:
        data['CATEGORY'] = 'LIFE_GOODS'
    print(x)
    print(data['CATEGORY'])
    # data['CATEGORY'] = random.choice([0, 's', real_category, brand])
    
    # timestamp
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['CLIENT_TIMESTAMP'] = str_now
    return data

def lambda_handler(event, context):
    
        while True:
            client.put_record(
                StreamName='ingest-lambda-click-streaming',
                Data=json.dumps(getReferrer()),
                PartitionKey='partitionkey_1')

