import json
import base64
import boto3

def lambda_handler(event, context):
    
    for record in event["Records"]:
        
        # decode message
        decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        print(decoded_data)
        
        # convert json string to json
        data = json.loads(decoded_data)
        
        # get the number of attribute
        num_attri = len(data)
        
        # check the number of attributes in dictionary
        if num_attri != 4:
            
            # print the number of attributes in logs
            print(f"The number of Attribute is {num_attri}. We expect 4!")
         
        # check if SESSION_ID exists
        try:
            data["SESSION_ID"]
        except KeyError:
            print("Missing SESSION_ID!")
        
        # check if ITEM_ID exists
        try:
            data["ITEM_ID"]
        except KeyError:
            print("Missing ITEM_ID!")
         
        # check if CATEGORY exists
        try:
            data["CATEGORY"]
        except KeyError:
            print("Missing CATEGORY!")
        
        # check if CLIENT_TIMESTAMP exists
        try:
            data["CLIENT_TIMESTAMP"]
        except KeyError:
            print("Missing CLIENT_TIMESTAMP!")
      
          
