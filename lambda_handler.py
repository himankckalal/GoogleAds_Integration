from base64 import decode
import json
import os
import pandas as pd
import tempfile
import resource
from unittest import result
from urllib import response
import urllib3
import hashlib
import csv
import uuid
import boto3
# import phonenumbers
# from s3urls import parse_url


http = urllib3.PoolManager()
import logging
logger = logging.getLogger() 
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
from urllib3._collections import HTTPHeaderDict

INPUT_KEY_BODY               = "body"
INPUT_KEY_HEADERS            = "headers"

HEADER_KEY_CONNECTION_INFO   = "ci360-conn-system-connector-attributes"
INPUT_HEADER_MANDATORY_KEYS  = [HEADER_KEY_CONNECTION_INFO]


HEADER_CLIENT_ID = 'Client ID'
HEADER_CLIENT_SECRET =  'Client secret'
HEADER_DEVELOPER_TOKEN = "Developer token"
HEADER_REFRESH_TOKEN = "Refresh token"
HEADER_LOGIN_CUSTOMER_ID = "Login customer ID" 


def lambda_handler(event, context):
    logger.info(f"Inside GoogleAdsCustomerMatch Lambda. Payload = '{event}'")

    # if event is None:
    #     logger.error("Null event received")
    #     return construct_response_body(400, "Null event received")
    # elif INPUT_KEY_BODY not in event:
    #     logger.error("Received empty body in event payload")
    #     return construct_response_body(400, "Received empty body in event payload")


    # Validate input for mandatory data items
    # headers = json.loads(event[INPUT_KEY_HEADERS])
    # headers = event[INPUT_KEY_HEADERS]
    # for key in INPUT_HEADER_MANDATORY_KEYS:
    #     if key not in headers:
    #         logger.error(f"Could not find '{key}' in headers")
    #         return construct_response_body(400, f"Could not find '{key}' in headers")
    #     else:
    #         logger.info(f"Found required header : name='{key}', value='{headers[key]}'")

    # connection_info_header_value = headers[HEADER_KEY_CONNECTION_INFO]
    # connection_info_json = decode_connection_info(connection_info_header_value)
    # developer_token = connection_info_json[HEADER_DEVELOPER_TOKEN]
    # client_id = connection_info_json[HEADER_CLIENT_ID]
    # client_secret = connection_info_json[HEADER_CLIENT_SECRET]
    # refresh_token = connection_info_json[HEADER_REFRESH_TOKEN]
    # login_customer_id = connection_info_json[HEADER_LOGIN_CUSTOMER_ID]

    # event_body = json.loads(event[INPUT_KEY_BODY])
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    refresh_token = os.environ.get('refresh_token')
    login_customer_id = os.environ.get('login_customer_id')
    developer_token = os.environ.get('developer_token')

    
    # event_body = event
    # Getting Bearer Token using client_id,client_secret and refresh_token
    s3_location = event["presigned_URL"]
    # s3_location = event["URI"]
    customer_id = event["customer_id"]


    bearer_token = get_authorization_token(client_id, client_secret, refresh_token)
    logger.info('printing get_authorization_token')
    logger.info(bearer_token)
    # Getting Resource_name for calling createOfflineUserDataJon
    resource_name = get_resource_name(developer_token, bearer_token, login_customer_id, customer_id)
    logger.info('printing get_resource_name')
    logger.info(resource_name)
    # Creating offline_user_data_job_resource_name
    offline_user_data_job_resource_name = create_offline_user_data_job(resource_name, developer_token, bearer_token, login_customer_id, customer_id)
    logger.info('printing offline_user_data_job_resource_name')
    logger.info(offline_user_data_job_resource_name)
    # Adding Operations(Create/Delete) for user data the actual json payload from csv
    add_operations_for_user_data(offline_user_data_job_resource_name, developer_token, bearer_token, login_customer_id, s3_location)
    run_offline_job_response = run_offline_job(offline_user_data_job_resource_name, developer_token, bearer_token, login_customer_id)
    final_response = construct_response_body(200, run_offline_job_response)        
    return final_response


def get_authorization_token(client_id, client_secret, refresh_token):
    # Getting all the required information for token creation from environmental variable
    #logger.info(os.environ.get('client_id'))
    logger.debug('client id in the request : {}'.format(client_id))
    #logger.info(os.environ.get('client_secret'))
    logger.debug('client secret in the request : {}'.format(client_secret))
    #logger.info(os.environ.get('refresh_token'))
    logger.debug('refresh token in the request : {}'.format(refresh_token))

    # creating json payload
    payload = {"grant_type" : "refresh_token",
        "client_id" : client_id,
        "client_secret" : client_secret,
        "refresh_token" : refresh_token
    }
    # calling api using POST request
    response = http.request('POST', 'https://www.googleapis.com/oauth2/v3/token',fields=payload)
    # decoding the response
    decoded_response = response.data.decode('utf-8')
    # converting respose into string 
    json_str = json.loads(decoded_response)
    # getting brarer token from response json
    bearer_token = json_str['access_token']
    return bearer_token        


def get_resource_name(developer_token, bearer_token, login_customer_id, customer_id):
    # creating payload
    # adding uuid component to recognize each req differently 
    payload = {
    "operations": [
        {
    "create": {
    "membershipLifeSpan": "3",
    "name" : "Uploaded from lambda_test - " + str(uuid.uuid4()),
    "crmBasedUserList" : {
    "uploadKeyType" : "CONTACT_INFO"
                        }
            }
        }
                ]
            }
    # creating json string from dictionay
    json_str = json.dumps(payload)
    # encoding same string to byte format
    encoded_json_str = json_str.encode('utf-8')
    
    # creating header with HTTPHeaderDict 
    # point to note!! must use different header name than header itself
    headers_dict = HTTPHeaderDict()
    headers_dict.add("Authorization", "Bearer " + str(bearer_token))
    headers_dict.add("developer-token", developer_token)
    headers_dict.add("Content-Type", "application/json")
    headers_dict.add("login-customer-id",  login_customer_id)
    api_url = 'https://googleads.googleapis.com/v10/customers/{customer_id}/userLists:mutate'.format( customer_id = customer_id)
    response = http.request('POST',api_url,headers=headers_dict,body=encoded_json_str)
    # decoding response
    decoded_obj = response.data.decode('utf-8')
    # converting  decoded response to json string
    json_str = json.loads(decoded_obj)
    # extracting resoucename from json
    resource_name = json_str['results'][0]['resourceName']
    str1 = 'get_resouce_name{test}'.format(test = resource_name)
    logger.info(str1)
    return resource_name


def generate_add_users_input(s3_location):

    # response = http.request("GET",s3_location)
    # data = response.data.decode('utf-8').splitlines()
    # f = data
   
    # bucket,key = get_bucket_and_key_from_s3(s3_location)
    # s3.download_file(bucket,key,'/tmp/mycsv.csv')
    # f = open( '/tmp/mycsv.csv', 'rU' )


    df = pd.read_csv(s3_location)
    # logger.info(df.head())
    logger.info(df.columns)
    
    operations = []
    for index, row in df.iterrows():
        userIdentifier = [] 
        
        phone_numbers = str(row["Phone"]).split("/")
        # print(phone_numbers)
        for phone_number in phone_numbers:
            hashedPhoneNumber = normalize_and_sha256('+91'+(phone_number))
            hashedphone ={"hashedPhoneNumber" : hashedPhoneNumber} 
            userIdentifier.append(hashedphone)
            
        if "Email" in df.columns:
            if '@' in str(row["Email"]):
                email = normalize_and_sha256(row["Email"])
                # email = row["Email"]
                hashedemail = {"hashedEmail" : email}
                userIdentifier.append(hashedemail)
        
        if "First Name" in df.columns and "Last Name" in df.columns and "Zip" in df.columns and "Country" in df.columns:
            print(row)
            if not pd.isnull(df.at[index, 'First Name']) and not pd.isnull(df.at[index, 'Last Name']) and not pd.isnull(df.at[index, 'Zip'])  and  not pd.isnull(df.at[index, 'Country']):
                first_name = normalize_and_sha256(row["First Name"])
                last_name = normalize_and_sha256(row["Last Name"])
                address = {
                    "hashedFirstName": first_name ,
                    "hashedLastName": last_name ,
                    "countryCode": row["Country"],
                    "postalCode": row["Zip"]
                }   
                address_dict = { "addressInfo" : address }
                userIdentifier.append(address_dict)
        userIdentifier = {
                "userIdentifiers": userIdentifier
            }
        
        create_dict = { "create" : userIdentifier }
        operations.append(create_dict)
        
    operations_dict = { "operations" : operations}
        
    out = json.dumps(operations_dict, indent=4)
    logger.info("printing oout")
    logger.info(out)
    return out
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    
    # reader = csv.DictReader(f, fieldnames=(['Email','First Name','Last Name','Country','Zip','Phone']))
    # next(reader)
    # operations = []
    # for row in reader:
    #     userIdentifier = [] 
    #     check = 0
    #     phone_numbers = str(row["Phone"]).split("/")
    #     for phone_number in phone_numbers:
    #         hashedPhoneNumber = normalize_and_sha256('+91'.join(phone_number))
    #         hashedphone ={"hashedPhoneNumber" : hashedPhoneNumber} 
    #         userIdentifier.append(hashedphone)

    #     if '@' in str(row["Email"]):
    #         email = normalize_and_sha256(row["Email"])
    #         hashedemail = {"hashedEmail" : email}
    #         userIdentifier.append(hashedemail)
    
    #     if check == 0 and (row["First Name"] or row["Last Name"] or row["Zip"] or  row["Country"]):
    #         check = 1
    #         if row["First Name"] and row["Last Name"] and row["Zip"] and row["Country"]:
    #             first_name = normalize_and_sha256(row["First Name"])
    #             last_name = normalize_and_sha256(row["Last Name"]) 
    #             address = {
    #             "hashedFirstName": first_name,
    #             "hashedLastName": last_name,
    #             "countryCode": row["Country"],
    #             "postalCode": row["Zip"]
    #         }   
    #         address_dict = { "addressInfo" : address }
    #         userIdentifier.append(address_dict)
    
    #     userIdentifier = {
    #             "userIdentifiers": userIdentifier
    #         }
    #     create_dict = { "create" : userIdentifier }
    #     operations.append(create_dict)
    # operations_dict = { "operations" : operations}
        
    # out = json.dumps(operations_dict, indent=4)
    # return out


def create_offline_user_data_job(userlist_resource_name, developer_token, bearer_token, login_customer_id, customer_id):
    # creating reqeust header 
    req_headers = {
        'developer-token': developer_token,
        'login-customer-id': login_customer_id,
        'Accept': 'application/json',
        'Authorization':  "Bearer " + str(bearer_token),
        }

    # creating Payload 
    # sample userlist  'userList': 'customers/4813827784/userLists/7215093078',
    payload = {
        'job': {
            'type': 'CUSTOMER_MATCH_USER_LIST',
            'customerMatchUserListMetadata': {
                
                'userList': userlist_resource_name,
            },
        },
        }
    # Creating json from dict
    json_data = json.dumps(payload)
    # Encoding the json created
    json_data = json_data.encode('utf-8')
    # Getting customer_id from environment variables
    # customer_id = os.environ.get('customer_id')
    # appending customer_id into url
    api_url = 'https://googleads.googleapis.com/v10/customers/{customer_id}/offlineUserDataJobs:create'.format(customer_id = customer_id)
    response = http.request('POST',api_url, headers=req_headers, body=json_data)
    # Decoding Response
    decoded_response = response.data.decode('utf-8')
    # creating json string
    json_string = json.loads(decoded_response)
    # getting resource_name from json string
    resource_name = json_string['resourceName']
    # returning resource_name
    # str2 = 'create_offline_user_data_job {test}'.format(test = resource_name)
    # logger.info(str2)
    return resource_name


def add_operations_for_user_data(offline_user_data_job_resource_name, developer_token, bearer_token, login_customer_id, s3_location):
    # Creating request header
    req_headers = {
        'developer-token': developer_token,
        'login-customer-id': login_customer_id,
        'Accept': 'application/json',
        'Authorization':  "Bearer " + str(bearer_token),
        }

    # creating json for input csv file 
    json_data = generate_add_users_input(s3_location)
    logger.info("json_data")
    logger.info(json_data)
        # encoding json data
    json_data = json_data.encode('utf-8')
    api_url = 'https://googleads.googleapis.com/v10/{offline_user_data_job_resource_name}:addOperations'.format(offline_user_data_job_resource_name = offline_user_data_job_resource_name)
        
    response = http.request('POST',api_url, headers=req_headers, body=json_data)
    logger.info(response.data)
        
              
def run_offline_job(offline_user_data_job_resource_name, developer_token, bearer_token,login_customer_id):
    # creating request header
    req_headers = {
    'developer-token': developer_token,
    'login-customer-id': login_customer_id,
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization':  "Bearer " + str(bearer_token),
        }
    api_url = 'https://googleads.googleapis.com/v10/{offline_user_data_job_resource_name}:run'.format(offline_user_data_job_resource_name = offline_user_data_job_resource_name)
    response = http.request('POST',api_url, headers=req_headers)
    # decoding response
    decoded_response = response.data.decode('utf-8')
    # creating json string from decoded response
    json_str = json.loads(decoded_response)
    logger.info('printing run offline')
    logger.info(json_str)
    return json_str
    

def normalize_and_sha256(s):
    return hashlib.sha256(s.strip().lower().encode()).hexdigest()


# def get_bucket_and_key_from_s3(s3_location):
#     flg = 0
#     s3location_string = s3_location + '\n'
#     for i in range(0,len(s3location_string)):
#         if s3location_string[i] == '/' and s3location_string[i+1]=='/': 
#             bucket=''
#             i = i + 2
#             while s3location_string[i]!='.':
#                 bucket = bucket+(s3location_string[i])
#                 i = i + 1
        
#         if flg ==0 and s3location_string[i] == '/' and s3location_string[i-1]!='/':
#             key = ''
#             flg = 1
#             i = i + 1
#             while  s3location_string[i]!='?' and s3location_string[i]!='\n':
#                 key = key + (s3location_string[i])
#                 i = i + 1
        
#     print(bucket)
#     print(key)
#     return bucket,key
        
        
def construct_response_body(status_code, response_body):
    return {
                'statusCode': status_code,
                'body': response_body
            }


def decode_connection_info(connection_info_header_value):
    decoded_connection_info = base64.b64decode(connection_info_header_value)
    connection_info_json = json.loads(decoded_connection_info)
    return connection_info_json
