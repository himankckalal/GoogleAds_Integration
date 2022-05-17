from base64 import decode
import json
import os
import resource
from unittest import result
from urllib import response
import urllib3
import hashlib
import csv
import uuid
http = urllib3.PoolManager()
import logging
logger = logging.getLogger() 
logger.setLevel(logging.INFO)



from urllib3._collections import HTTPHeaderDict
# import requests

def get_authorization_token():
    # Getting all the required information for token creation from environmental variable
    logger.info(os.environ.get('client_id'))
    logger.info(os.environ.get('client_secret'))
    logger.info(os.environ.get('refresh_token'))
    
    # creating json payload
    payload = {"grant_type" : "refresh_token",
    "client_id" : os.environ.get('client_id'),
    "client_secret" : os.environ.get('client_secret'),
    "refresh_token" : os.environ.get('refresh_token')
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


def get_resource_name(bearer_token):
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
    headers1 = HTTPHeaderDict()
    headers1.add("Authorization", "Bearer " + str(bearer_token))
    headers1.add("developer-token", os.environ.get('developer_token'))
    headers1.add("Content-Type", "application/json")
    headers1.add("login-customer-id",  os.environ.get('login_customer_id'))
    

    api_url = 'https://googleads.googleapis.com/v10/customers/{customer_id}/userLists:mutate'.format( customer_id = os.environ.get('customer_id'))
    response = http.request('POST',api_url,headers=headers1,body=encoded_json_str)
    # decoding response
    decoded_obj = response.data.decode('utf-8')
    # converting  decoded response to json string
    json_str = json.loads(decoded_obj)
    # extracting resoucename from json
    resource_name = json_str['results'][0]['resourceName']
    str1 = 'get_resouce_name{test}'.format(test = resource_name)
    logger.info(str1)

    return resource_name



def normalize_and_sha256(s):
    return hashlib.sha256(s.strip().lower().encode()).hexdigest()
    
    
def generate_add_user_input_for_only_phone_numbers():
    csvFilePath = r'./random_numbers.csv'
    f = open( csvFilePath, 'rU' )
    # Declare the column name from csv
    reader = csv.DictReader(f, fieldnames=(['Phone']))
    operations = []
    for row in reader:
        userIdentifier = []
        if row["Phone"] == "Phone":
            continue
        if row["Phone"]:
            phone = normalize_and_sha256(row["Phone"])
            hashedphone ={"hashedPhoneNumber" : phone} 
            userIdentifier.append(hashedphone)
        userIdentifier = {
            "userIdentifiers": userIdentifier
        }
        create_dict = { "create" : userIdentifier }
        operations.append(create_dict)
        operations_dict = { "operations" : operations}
    
    out = json.dumps(operations_dict, indent=4)
    # logger.info(out)
    return out
def generate_add_users_input():
    # Declaring json input file path
    csvFilePath = r'./random_numbers.csv'
    f = open( csvFilePath, 'rU' )
    # Declare the column name from csv
    reader = csv.DictReader(f, fieldnames=(['Email','First Name','Last Name','Country','Zip','Phone']))
    operations = []
    for row in reader:
        userIdentifier = []
        if row["Email"] == "Email":
            continue
        if row["Phone"]:
            phone = normalize_and_sha256(row["Phone"])
            hashedphone ={"hashedPhoneNumber" : phone} 
            userIdentifier.append(hashedphone)

        if row["Email"]:
            email = normalize_and_sha256(row["Email"])
            hashedemail = {"hashedEmail" : email}
            userIdentifier.append(hashedemail)

        if row["First Name"] and row["Last Name"] and row["Zip"] and row["Country"]:
            first_name = normalize_and_sha256(row["First Name"])
            last_name = normalize_and_sha256(row["Last Name"]) 
            address = {
                "hashedFirstName": first_name,
                "hashedLastName": last_name,
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
    return out

def createOfflineUserDataJob(userlistResourceName,bearer_token):
    # creating reqeust header 
    req_headers = {
    'developer-token': os.environ.get('developer_token'),
    'login-customer-id': os.environ.get('login_customer_id'),
    'Accept': 'application/json',
    'Authorization':  "Bearer " + str(bearer_token),
    }

    # creating Payload 
    # sample userlist  'userList': 'customers/4813827784/userLists/7215093078',
    payload = {
    'job': {
        'type': 'CUSTOMER_MATCH_USER_LIST',
        'customerMatchUserListMetadata': {
            
            'userList': userlistResourceName,
        },
    },
    }
    # Creating json from dict
    json_data = json.dumps(payload)
    # Encoding the json created
    json_data = json_data.encode('utf-8')
    # Getting customer_id from environment variables
    customer_id = os.environ.get('customer_id')
    # appending customer_id into url
    api_url = 'https://googleads.googleapis.com/v10/customers/{customer_id}/offlineUserDataJobs:create'.format(customer_id =customer_id)
    response = http.request('POST',api_url, headers=req_headers, body=json_data)
    # Decoding Response
    decoded_response = response.data.decode('utf-8')
    # creating json string
    json_string = json.loads(decoded_response)
    # getting resource_name from json string
    resource_name = json_string['resourceName']
    # returning resource_name
    # str2 = 'createOfflineUserDataJob {test}'.format(test = resource_name)
    # logger.info(str2)
    return resource_name


def addOperationsForUserData(offline_user_data_job_resource_name,bearer_token):
    # Creating request header
    req_headers = {
    'developer-token': os.environ.get('developer_token'),
    'login-customer-id': os.environ.get('login_customer_id'),
    'Accept': 'application/json',
    'Authorization':  "Bearer " + str(bearer_token),
    }
    # creating json for input csv file 
    json_data = generate_add_user_input_for_only_phone_numbers()
    # json_data = generate_add_users_input()
    logger.info("json_data")
    logger.info(json_data)
    # encoding json data
    json_data = json_data.encode('utf-8')
    api_url = 'https://googleads.googleapis.com/v10/{offline_user_data_job_resource_name}:addOperations'.format(offline_user_data_job_resource_name = offline_user_data_job_resource_name)
    
    response = http.request('POST',api_url, headers=req_headers, body=json_data)
    logger.info(response.data)
    
    
   
    
    
def runOfflineJob(offline_user_data_job_resource_name,bearer_token):
    # creating request header
    req_headers = {
    'developer-token': os.environ.get('developer_token'),
    'login-customer-id': os.environ.get('login_customer_id'),
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
   
    
def lambda_handler(event, context):
    # Getting Bearer Token using client_id,client_secret and refresh_token
    bearer_token = get_authorization_token()
    logger.info('printing get_authorization_token')
    logger.info(bearer_token)
    # Getting Resource_name for calling createOfflineUserDataJon
    resource_name = get_resource_name(bearer_token)
    logger.info('printing get_resource_name')
    logger.info(resource_name)
    # Creating offline_user_data_job_resource_name
    offline_user_data_job_resource_name = createOfflineUserDataJob(resource_name ,bearer_token)
    logger.info('printing offline_user_data_job_resource_name')

    logger.info(offline_user_data_job_resource_name)
    # Adding Operations(Create/Delete) for user data the actual json payload from csv
    addOperationsForUserData(offline_user_data_job_resource_name,bearer_token)
    final_response = runOfflineJob(offline_user_data_job_resource_name,bearer_token)
    
    return {
        'statusCode': 200,
        'body': json.dumps(final_response)
    }
