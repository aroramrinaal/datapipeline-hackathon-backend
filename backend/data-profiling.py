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

# Set environment variables (consider moving these to Lambda environment variables)
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
        Analyze this CSV data and provide a detailed data profiling analysis. Focus on:
        1. Column-level analysis including data types, missing values, and basic statistics
        2. Data quality issues and potential anomalies
        3. Distribution patterns and outliers
        4. Relationships between columns
        5. Potential join keys and data integration opportunities
        
        Format the response as a clean JSON without any markdown or explanatory text.
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
    url = os.environ.get("litellm_proxy_endpoint") + "/chat/completions"
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'x-api-key': os.environ.get('LITELLM_API_KEY')
    }
    payload = {
        "model": "Azure OpenAI GPT-4o (External)",
        "messages": [
            {"role": "system", "content": "You are a data profiling specialist. Provide concise, accurate analysis."},
            {"role": "user", "content": prepare_message(csv_data)},
        ],
        "temperature": 0.3,
        "max_tokens": 2048
    }

    response = requests.post(url, headers=headers, json=payload, verify=True)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"LiteLLM API Error: {response.status_code} - {response.text}")

# Lambda Handler: Entry point for the Lambda function
def lambda_handler(event, context):
    """Main Lambda Handler."""
    # Match the metadata Lambda's CORS headers exactly
    cors_headers = {
        'Access-Control-Allow-Origin': 'http://localhost:5173',  # Specific origin instead of '*'
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key,Authorization',
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Expose-Headers': '*'
    }
    
    # Handle preflight OPTIONS request - match metadata Lambda's approach
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': ''
        }
    
    try:
        # Parse the request body
        print("Received event:", json.dumps(event))
        
        body = json.loads(event.get('body', '{}'))
        file_name = body.get('fileName')  # Note: matches frontend's key name
        
        if not file_name:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'fileName is required in the request body'
                })
            }
            
        bucket_name = "uploadedcsvfiles"
        
        print(f"Processing file: {file_name} from bucket: {bucket_name}")
        
        try:
            # Read CSV data
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            csv_data = response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Failed to read CSV: {str(e)}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': f'Failed to read CSV file: {str(e)}'
                })
            }
            
        try:
            # Get Cognito token and process data
            bearer_token = get_cognito_token()
            chunks = chunk_csv_data(csv_data)
            all_analyses = []

            for chunk in chunks:
                response = call_lite_llm_api(chunk, bearer_token)
                content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
                try:
                    analysis = json.loads(content)
                    all_analyses.append(analysis)
                except json.JSONDecodeError:
                    continue

            final_analysis = merge_analyses(all_analyses)
            
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Data profiling completed successfully',
                    'fileName': file_name,
                    'analysis': final_analysis
                })
            }
            
        except Exception as e:
            print(f"Failed to generate profiling: {str(e)}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': f'Failed to generate profiling: {str(e)}'
                })
            }
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Failed to process request: {str(e)}'
            })
        }

def merge_analyses(analyses):
    """
    Intelligently merge multiple analyses into a single coherent report
    """
    if not analyses:
        return {}
    
    merged = {
        "columns": {},
        "overall_statistics": {},
        "relationships": [],
        "data_quality_issues": []
    }
    
    # Merge column analyses
    for analysis in analyses:
        for col_info in analysis.get("columns", []):
            col_name = col_info.get("name")
            if col_name not in merged["columns"]:
                merged["columns"][col_name] = col_info
            else:
                # Update statistics if they exist
                if "statistics" in col_info:
                    merged["columns"][col_name]["statistics"].update(col_info["statistics"])
                # Append unique notes
                if "notes" in col_info:
                    existing_notes = set(merged["columns"][col_name].get("notes", "").split(". "))
                    new_notes = set(col_info["notes"].split(". "))
                    merged["columns"][col_name]["notes"] = ". ".join(existing_notes.union(new_notes))

    # Convert columns back to list
    merged["columns"] = list(merged["columns"].values())
    
    return merged

# Read CSV from S3 (if file_path is an S3 path)
def read_csv_to_string(file_path):
    try:
        s3_client = boto3.client('s3')
        bucket_name = 'uploadedcsvfiles'
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        raise Exception(f"Failed to read CSV from S3: {e}")
