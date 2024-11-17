import os
import json
import hmac
import hashlib
import base64
import requests
import boto3
from botocore.exceptions import ClientError
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# Set environment variables
os.environ['COGNITO_CLIENT_SECRET'] = '11181idr8ho95i6k67v99p3go2sn72kri5j5qdg2lo4jl0uj9ri4'
os.environ['COGNITO_PASSWORD'] = 'Aarav@1209'
os.environ['COGNITO_USERNAME'] = 'amatalia@asu.edu'
os.environ['COGNITO_USER_POOL_ID'] = 'us-east-1_7Rq8X1q2q'
os.environ['COGNITO_CLIENT_ID'] = '7v15em5a0iqvb3hn5r69cg3485'
os.environ['litellm_proxy_endpoint'] = "https://api-llm.ctl-gait.clientlabsaft.com"
os.environ['LITELLM_API_KEY'] = 'Bearer sk-_UCJBhDuE_YPLi4B4gxASQ'

# Helper function to calculate the secret hash
def calculate_secret_hash(username, client_id, client_secret):
    message = username + client_id
    dig = hmac.new(
        key=client_secret.encode('utf-8'),
        msg=message.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(dig).decode()

# Helper function to get Cognito token
def get_cognito_token():
    try:
        # Initialize Cognito Identity Provider client
        client = boto3.client('cognito-idp', region_name='us-east-1')
        
        # Get credentials from environment variables
        client_id = os.environ.get('COGNITO_CLIENT_ID')
        client_secret = os.environ.get('COGNITO_CLIENT_SECRET')
        username = os.environ.get('COGNITO_USERNAME')
        password = os.environ.get('COGNITO_PASSWORD')
        
        # Calculate secret hash
        secret_hash = calculate_secret_hash(username, client_id, client_secret)
        
        # Initiate auth flow
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

# Prepare the message for the API request
def prepare_message(csv_data):
    return """
        Analyze the following CSV data and return ONLY a JSON object containing the in-depth data profiling analysis of the dataset. 
        Do not include any explanatory text before or after the JSON. 
        The JSON should be in the following exact format:

        {
        "columns": [
            {
            "name": "[Column Name]",
            "data_type": "[Numerical/Categorical/Date/etc.]",
            "missing_values": {
                "count": [Count],
                "percentage": [Percentage]
            },
            "statistics": {
                "mean": [Value],
                "median": [Value],
                "mode": [Value],
                "min": [Value],
                "max": [Value]
            },
            "notes": "[Outliers, unusual distributions, or potential data quality issues]"
            },
            ...
        ],
        "similarity_analysis": {
            "overlapping_columns": ["Column1", "Column2"],
            "schema_matching": "[Description of schema similarities]"
        },
        "join_analysis": {
            "common_fields": ["Field1", "Field2"],
            "query": "SELECT [columns] FROM [Dataset1] JOIN [Dataset2] ON [common_field];",
            "type": "[Inner/Outer/Left/Right]",
            "use_case": "[Description of join's value]"
        }
        }

        Here is the CSV data for analysis:
    """ + f"""{csv_data}"""

# Chunk the data if necessary
def chunk_csv_data(csv_data, max_chunk_size=100000):
    chunks = []
    current_chunk = ''
    for line in csv_data.splitlines():
        if len(current_chunk) + len(line) < max_chunk_size:
            current_chunk += line + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = line + "\n"
    if current_chunk:
        chunks.append(current_chunk)
    return chunks

# Directly calling the LiteLLM API
def call_lite_llm_api(csv_data, bearer_token):
    url = os.environ.get("litellm_proxy_endpoint", "https://api-llm.ctl-gait.clientlabsaft.com") + "/chat/completions"
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'x-api-key': os.environ.get('LITELLM_API_KEY')
    }
    payload = {
        "model": "Azure OpenAI GPT-4o (External)",
        "messages": [
            {"role": "system", "content": "You are a data analysis assistant."},
            {"role": "user", "content": prepare_message(csv_data)},
        ],
        "temperature": 0.5,
        "max_tokens": 2048
    }

    # Make the API call
    response = requests.post(url, headers=headers, json=payload, verify=True)
    
    # Check for success response
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error from LiteLLM API: {response.status_code} - {response.text}")

# Lambda Handler: Entry point for the Lambda function
def lambda_handler(event, context):
    try:
        # Retrieve the CSV data from the event or S3 bucket
        file_path = event.get('file_path')  # or event['csv_data']
        
        # Read CSV data (or retrieve from S3, etc.)
        csv_data = read_csv_to_string(file_path)
        
        # Chunk the data if needed
        chunk_data = chunk_csv_data(csv_data)

        # Get Cognito Token
        bearer_token = get_cognito_token()

        # Call LiteLLM API to analyze CSV data
        responses = []
        for chunk in chunk_data:
            response = call_lite_llm_api(chunk, bearer_token)
            responses.append(response)

        # Merge all responses into a final report
        merged_response = "\n".join([response.get("choices", [{}])[0].get("message", {}).get("content", "") for response in responses])

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data profiling completed successfully.',
                'data_profiling_report': merged_response
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to generate data profiling report.',
                'error': str(e)
            })
        }

# Read CSV from S3 (if file_path is an S3 path)
def read_csv_to_string(file_path):
    try:
        s3_client = boto3.client('s3')
        bucket_name = 'uploadedcsvfiles'
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        raise Exception(f"Failed to read CSV from S3: {e}")
