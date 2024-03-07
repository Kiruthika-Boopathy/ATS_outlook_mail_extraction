import json

import boto3
import html2text
import msal
from msal import ConfidentialClientApplication
import os
import uuid
import requests
import webbrowser
import mysql.connector


def get_access_token():
    email = "jobs_della@vrdella.com"
    app_id = 'c3ee5854-d6b5-446c-b330-31fbf7213227'
    client_secret = 'WLv8Q~A1H5JKeMseYnMgcThTXLhOasKHO56EWdlt'

    SCOPES = ['User.Read', 'Mail.Read']
    cache_file_path = f'token_cache_{email.replace("@", "_").replace(".", "_")}.bin'
    token_cache = msal.SerializableTokenCache()
    if os.path.exists(cache_file_path):
        token_cache.deserialize(open(cache_file_path, "r").read())

    client = ConfidentialClientApplication(
        client_id=app_id,
        client_credential=client_secret,
        token_cache=token_cache
    )

    accounts = client.get_accounts()
    if accounts:
        result = client.acquire_token_silent(SCOPES, account=accounts[0])
        access_token = result.get("token_datas")
    else:
        authorization_url = client.get_authorization_request_url(SCOPES)
        webbrowser.open(authorization_url)
        authorization_code = input("Enter the authorization code: ")
        result = client.acquire_token_by_authorization_code(authorization_code, SCOPES)
        access_token = result.get("token_datas")
    open(cache_file_path, "w").write(token_cache.serialize())
    headers = {
        'Authorization': f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    print(headers)
    return headers


aws_accessKey = "AKIAULJPPD7NP2MNM4NI"
aws_secretKey = "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"
secret_response = {}
session = boto3.Session(aws_access_key_id=aws_accessKey, aws_secret_access_key=aws_secretKey)


def Lambda_functioncall():
    try:
        headers = get_access_token()
        lambda_client = session.client('lambda', region_name='ap-south-1')
        # convert the dictionary to a JSON string
        payload = json.dumps(headers)
        # invoke the Lambda function with the payload
        response = lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:105464463873:function:VMS_Prowand',
            Payload=payload
        )
        response_payload = response['Payload'].read().decode("utf-8")
        ocr_pdf_encode_data = json.loads(json.loads(response_payload)['data'])[2:]
        print(ocr_pdf_encode_data)
        return ocr_pdf_encode_data

    except Exception as e:
        print("File Conversion process is failed.", str(e))


Lambda_functioncall()
