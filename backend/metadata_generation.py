import os
import json
import boto3
import hmac
import hashlib
import base64
from botocore.exceptions import ClientError
import requests
import warnings
from langchain.chat_models import ChatOpenAI
from langchain.schema import HumanMessage,SystemMessage

# Suppress warnings
warnings.filterwarnings("ignore")

# Environment Variables
os.environ['COGNITO_CLIENT_SECRET'] = '11181idr8ho95i6k67v99p3go2sn72kri5j5qdg2lo4jl0uj9ri4'
os.environ['COGNITO_PASSWORD'] = 'HelloWorld#1'
os.environ['COGNITO_USERNAME'] = 'sjain300@asu.edu'
os.environ['COGNITO_USER_POOL_ID'] = 'us-east-1_7Rq8X1q2q'
os.environ['COGNITO_CLIENT_ID'] = '7v15em5a0iqvb3hn5r69cg3485'
os.environ['litellm_proxy_endpoint'] = 'https://api-llm.ctl-gait.clientlabsaft.com'

def calculate_secret_hash(username, client_id, client_secret):
    """Generate Cognito Secret Hash."""
    message = username + client_id
    dig = hmac.new(
        key=client_secret.encode('utf-8'),
        msg=message.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(dig).decode()

def get_cognito_token():
    """Retrieve Cognito Access Token."""
    try:
        client = boto3.client('cognito-idp', region_name='us-east-1')
        client_id = os.environ.get('COGNITO_CLIENT_ID')
        client_secret = os.environ.get('COGNITO_CLIENT_SECRET')
        username = os.environ.get('COGNITO_USERNAME')
        password = os.environ.get('COGNITO_PASSWORD')
        secret_hash = calculate_secret_hash(username, client_id, client_secret)
        
        response = client.initiate_auth(
            ClientId=client_id,
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': username,
                'PASSWORD': password,
                'SECRET_HASH': secret_hash
            }
        )
        return response['AuthenticationResult']['AccessToken']
    except ClientError as e:
        raise Exception(f"Failed to authenticate with Cognito: {e}")

def read_csv_from_s3(bucket_name, file_key):
    """Read CSV file content from S3."""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        raise Exception(f"Failed to read file from S3: {e}")

        

def generate_metadata_via_llm(csv_data):
    """Generate metadata using LiteLLM."""
    lite_llm_url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
    headers = {
        "x-api-key": "Bearer sk-_UCJBhDuE_YPLi4B4gxASQ",
        "Authorization": f"Bearer {get_cognito_token()}"
    }
    prompt = f"""
    You are a data analysis assistant. The following is a CSV dataset. Please generate metadata:
    - Total rows and columns.
    - Column names and their data types.
    - Missing value counts for each column.
    - Unique value counts for each column.

    CSV data:
    {csv_data[:1000]}

    Please return structured metadata in JSON format.
    """
    payload = {
        "messages": [{"role": "user", "content": [ 
                { 
                    "type": "text", 
                    "text": f"{prompt}"
                } 
            ] }],
        "model": "Azure OpenAI GPT-4o (External)",
        "temperature": 0,
    }
    try:
        response = requests.post(lite_llm_url, headers=headers, data=json.dumps(payload), verify=True)
        return response.text
    except requests.exceptions.RequestException as e:
        return str(e)
        print(f"Error: {e}")
        if e.response:
            print(f"Response Status Code: {e.response.status_code}")
            print(f"Response Content: {e.response.text}")
        return None


def save_metadata_to_s3(bucket_name, file_key, metadata):
    """Save metadata as a JSON file in S3."""
    try:
        s3_client = boto3.client('s3')
        metadata_file_key = file_key.replace(".csv", "_metadata.json")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_file_key,
            Body=json.dumps(metadata, indent=2),
            ContentType="application/json"
        )
        return metadata_file_key
    except Exception as e:
        raise Exception(f"Failed to save metadata to S3: {e}")

def lambda_handler(event, context):
    """Main Lambda Handler."""
    try:
        # Extract S3 Bucket and File Path from Event
        file_path = event['file_path']
        bucket_name = "uploadedcsvfiles"
        
        # Read the CSV file
        csv_data = read_csv_from_s3(bucket_name, file_path)
        
        # Generate Metadata via LLM
        metadata = generate_metadata_via_llm(csv_data)

        # Save metadata to a s3
        metadata_file_key = save_metadata_to_s3(bucket_name, file_path, metadata)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Metadata generated successfully.',
                'metadata': metadata
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to generate metadata.',
                'error': str(e)
            })
        }

