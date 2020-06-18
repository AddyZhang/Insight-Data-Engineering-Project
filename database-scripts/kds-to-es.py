import boto3
import re
import requests
import json
import base64
from requests_aws4auth import AWS4Auth

# get awsauth
region = 'us-west-2' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# es host endpoint and index
host = 'https://search-my-s3-click-streaming-6xij4bhdyz6oahhrorq75jbb7u.us-west-2.es.amazonaws.com' # the Amazon ES domain, including https://
index = 'test8-index-'

# es headers
headers = { "Content-Type": "application/json" }

# Extract Category
def find_category(item_id):
    
    item_id = int(item_id)
    num = int(item_id/50)
    if num == 1:
        return 'missing_value'
    elif num == 2:
        return 'special_offer'
    elif num == 3:
        return 'furniture'
    elif num == 4:
        return 'electronics'
    elif num == 5:
        return 'sports'
    else:
        return 'life_goods'

# handle streaming data
def lambda_handler(event, context):
    
    for record in event["Records"]:
        
        # decode message
        decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        
        # convert json string to json
        data = json.loads(decoded_data)
        
        # get time data
        try:
            # split timestamp
            time_list = data["CURRENT_ROW_TIMESTAMP"].split()
        except KeyError:
            print("Missing CURRENT_ROW_TIMESTAMP or can't split!")
        
        # add T to let Kibana recognize timefield
        timestamp = time_list[0]+'T'+time_list[1]
        
        # get item id
        try:
            item_id = data["ITEM"]
        except KeyError:
            print("Missing ITEM!")
        
        # get item count
        try:
            item_count = data["ITEM_COUNT"]
        except KeyError:
            print("Missing ITEM!")
        
        # get document message
        document = {"timestamp":timestamp, "item_id":item_id, "item_count":int(item_count)}
        
        # print out message
        print(json.dumps(document))
        
        # get url
        category = find_category(item_id)
        type = 'index_type'
        url = host + '/' + index + category + '/' + type
        print(url)
        
        # send data to es endpoints    
        r = requests.post(url, auth=awsauth, json=document, headers=headers)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

