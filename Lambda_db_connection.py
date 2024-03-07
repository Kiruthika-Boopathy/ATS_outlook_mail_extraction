import boto3
import os
import json



secret_response = {}

try:
    session = boto3.Session(aws_access_key_id=os.environ['aws_accessKey'], aws_secret_access_key=os.environ['aws_secretKey'])
    client = session.client(
        service_name='secretsmanager',
        region_name=os.environ['region_name']
    )
    get_secret_value_response = client.get_secret_value(SecretId=os.environ['rds_secret_name'])
    secret_response = json.loads(get_secret_value_response['SecretString'])
except Exception as e:
    session = ''
    data = {"response_code": 500, "message": {"trace": str(e)}}
    print(data)


db_config = {
    'host': secret_response['db_host'] ,
    'user': secret_response['db_user'],
    'password': secret_response['db_password'],
    'database': secret_response['db_name']
}