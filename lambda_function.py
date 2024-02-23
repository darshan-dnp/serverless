import json
import requests
import os
import boto3
import json
import base64
from datetime import datetime
import uuid
from google.oauth2 import service_account
from google.cloud import storage

PROJECT_ID = os.environ.get('PROJECT_ID')
GOOGLE_ACCESS_KEY_ID = os.environ.get('GOOGLE_ACCESS_KEY_ID')
GOOGLE_BUCKET_NAME = os.environ.get('GOOGLE_BUCKET_NAME')
EMAIL_API = os.environ.get('EMAIL_API')
MAIL_DOMAIN = os.environ.get('MAIL_DOMAIN')
SECRET_ARN = os.environ.get('SECRET_ARN')
TABLE_NAME = os.environ.get('TABLE_NAME')
PATH = os.environ.get('PATH')
EMAIL_LIST = os.environ.get('EMAIL_LIST')

def save_event(email, receiver_email, file_name, status):
    bt3_session = boto3.session.Session()
    bt3_dynamodb = bt3_session.client(
        service_name='dynamodb', 
        region_name="us-east-1"
        )
    
    item = {
        "ID": {"S": str(uuid.uuid4())},
        "posted_by": {"S": email},
        "sent_to": {"S": receiver_email},
        "filename": {"S": file_name},
        "status": {"S": status}
    }
    print(item)
    response = bt3_dynamodb.put_item(TableName=TABLE_NAME, Item=item)
    print("PutItem Response:", response)


def lambda_handler(event, context):

    bt3_session = boto3.session.Session()
    bt3_client = bt3_session.client(
        service_name='secretsmanager', 
        region_name="us-east-1"
        )
    
    sec_response = bt3_client.get_secret_value(SecretId="GCP_SA_SECRET_3")
    decoded = base64.b64decode(sec_response['SecretString'])
    utf8_creds = json.loads(decoded.decode('utf-8'))

    sc_creds = service_account.Credentials.from_service_account_info(utf8_creds)

    storage_client = storage.Client(
        project = PROJECT_ID,
        credentials= sc_creds
        )
    
    message = event['Records'][0]['Sns']['Message']
    json_message = json.loads(message)
    print(json_message)

    url = json_message['url']
    email = json_message['email']
    
    current_datetime = datetime.now()
    timeStamp = current_datetime.strftime("%m%d%y-%H%M%S")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            file_name = email + "_" + timeStamp

            gcp_bucket_name = GOOGLE_BUCKET_NAME
            gcp_file_path = f"{PATH}/{file_name}"
            
            bucket = storage_client.bucket(gcp_bucket_name)
            blob = bucket.blob(gcp_file_path)
            
            blob.upload_from_string(response.content)
            mail_data = {"from": f"Post User <mailgun@darshanpate.me>",
                            "to": f"{EMAIL_LIST}",
                            "subject": f"Submission Posted by {email}",
                            "html": "The submission has been posted successfully."}
            requests.post(
                    "https://api.mailgun.net/v3/darshanpate.me/messages",
                    auth=("api", EMAIL_API),
                    data=mail_data)
            
            save_event(email, EMAIL_LIST, file_name, "Success")

        else:
            mail_data = {"from": f"Post User <mailgun@darshanpate.me>",
                            "to": f"{EMAIL_LIST}",
                            "subject": f"Submission Posted by {email}",
                            "html": "Failed to fetch the submission file."}
            requests.post(
                    "https://api.mailgun.net/v3/darshanpate.me/messages",
                    auth=("api", EMAIL_API),
                    data=mail_data)
            save_event(email, EMAIL_LIST, file_name, "Error")
            
    except requests.exceptions.RequestException as ex:
        mail_data = {"from": f"Post User <mailgun@darshanpate.me>",
                        "to": f"{EMAIL_LIST}",
                        "subject": f"Submission Posted by {email}",
                        "html": f"Failed to fetch the submission file. {ex}"}
        requests.post(
                "https://api.mailgun.net/v3/darshanpate.me/messages",
                auth=("api", EMAIL_API),
                data=mail_data)
        save_event(email, EMAIL_LIST, file_name, "Error")