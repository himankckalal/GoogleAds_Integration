import json
import os 
import urllib3
import hashlib
import csv
import uuid





from urllib3._collections import HTTPHeaderDict
# import requests

def get_authorization_token():
    print(os.environ.get('client_id'))
    print(os.environ.get('client_secret'))
    print(os.environ.get('refresh_token'))
    
    http = urllib3.PoolManager()
    payload = {"grant_type" : "refresh_token",
    "client_id" : os.environ.get('client_id'),
    "client_secret" : os.environ.get('client_secret'),
    "refresh_token" : os.environ.get('refresh_token')
   }
    print(payload)
    r = http.request('POST', 'https://www.googleapis.com/oauth2/v3/token',fields=payload)
    print(r.data.decode('utf-8'))
    temp = r.data.decode('utf-8')
    json_obj = json.loads(temp)
    bearer_token = json_obj['access_token']
    return bearer_token


def get_resource_name():
    bearer_token = get_authorization_token()
    http = urllib3.PoolManager()  
    
    data_string = {
"operations": [
    {
"create": {
"membershipLifeSpan": "30",
"name" : "Uploaded from lambda_test - " + str(uuid.uuid4()),
"crmBasedUserList" : {
"uploadKeyType" : "CONTACT_INFO"
                    }
        }
    }
            ]
        } 
    data_string_new = json.dumps(data_string)
    data_string_new = data_string_new.encode('utf-8')
  
    
    
    headers1 = HTTPHeaderDict()
    headers1.add("Authorization", "Bearer " + str(bearer_token))
    headers1.add("developer-token", "vJDJuZ_vJwGTkGGlFl3jUw")
    headers1.add("Content-Type", "application/json")
    headers1.add("login-customer-id", "6145797768")
    
    
    r = http.request('POST','https://googleads.googleapis.com/v10/customers/4650111896/userLists:mutate',headers=headers1,body=data_string_new)
    
    
    temp = r.data.decode('utf-8')
    json_obj = json.loads(temp)
    
    
    print(json_obj)
    
    
    
    # print(brarer_token)
    print(r.status)
    result = json_obj['results'][0]['resourceName']
    return result, bearer_token



def normalize_and_sha256(s):
    return hashlib.sha256(s.strip().lower().encode()).hexdigest()
    
    
    
def generate_add_users_input(csvFilePath,resource):
    f = open( csvFilePath, 'rU' )
    reader = csv.DictReader(f, fieldnames=(['Email','First Name','Last Name','Country','Zip','Phone']))
    print(reader)
    operations = []
    
    # print(reader)
   
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

            # continue
        userIdentifier = {
            "userIdentifiers": userIdentifier
        }
        # print(userIdentifier)
        create_dict = { "create" : userIdentifier }
        operations.append(create_dict)
        operations_dict = { "operations" : operations}
    
    customerMatchUserListMetadata = {}
    customerMatchUserListMetadata["userList"] = resource
    operations_dict["customerMatchUserListMetadata"] = customerMatchUserListMetadata
    
    out = json.dumps(operations_dict, indent=4)
    return out

# Call the make_json function


    
def lambda_handler(event, context):
    
    resource, bearer_token = get_resource_name()
    print(bearer_token)
    http = urllib3.PoolManager()  
    
    headers1 = HTTPHeaderDict()
    headers1.add("Authorization", "Bearer " + str(bearer_token))
    headers1.add("developer-token", "vJDJuZ_vJwGTkGGlFl3jUw")
    headers1.add("Content-Type", "application/json")
    headers1.add("Accept", "application/json")
    headers1.add("login-customer-id", "6145797768")
   
 
    csvFilePath = r'./Customer_Match.csv'
    out = generate_add_users_input(csvFilePath,resource)
    print(out)
  
 
    r = http.request('POST','https://googleads.googleapis.com/v10/customers/4650111896:uploadUserData',headers=headers1,body=out)
    
    temp = r.data.decode('utf-8')
    json_obj = json.loads(temp)
    
    
    print(json_obj)
    
    print(r.status)
  
    
 
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
