import boto3
import re
import requests
import json
from requests_aws4auth import AWS4Auth

region = 'us-west-2' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-my-s3-click-streaming-6xij4bhdyz6oahhrorq75jbb7u.us-west-2.es.amazonaws.com' # the Amazon ES domain, including https://
index = 'test7-index-'

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

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
    
# Lambda execution starts here
def lambda_handler(event, context):

    print("the event is {}".format(event))
    
    for record in event['Records']:
        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print("the bucket is {}".format(bucket))
        print("the key is {}".format(key))
        
        # # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        print("The body is {}".format(body))
        lines = body.splitlines()
        for line in lines:
            # print("line is {}".format(line))
            line_decode = line.decode('ASCII')
            # print("line_decode is {}".format(line_decode))
            attri_list = line_decode.split(',')
            time_str = attri_list[0]
            time_list = time_str.split()
            timestamp = time_list[0]+'T'+time_list[1]
            item_id = attri_list[1]
            item_count = attri_list[2]
            # print("Item is {}".format(item))
            document = {"timestamp":timestamp, "item_id":item_id, "item_count":int(item_count)}
            print("Document is {}".format(document))
            
            category = find_category(item_id)
            type = 'index_type'
            
            url = host + '/' + index + category + '/' + type
            print(url)
            
            r = requests.post(url, auth=awsauth, json=document, headers=headers)
   
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
