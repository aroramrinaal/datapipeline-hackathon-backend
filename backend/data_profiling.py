import os
import hmac
import base64
import hashlib
import json
import requests
import boto3
from botocore.exceptions import ClientError
from pprint import pprint
import warnings
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# Suppress warnings
warnings.filterwarnings("ignore")

# Set environment variables
os.environ['COGNITO_CLIENT_SECRET'] = '11181idr8ho95i6k67v99p3go2sn72kri5j5qdg2lo4jl0uj9ri4'
os.environ['COGNITO_PASSWORD'] ='HelloWorld#1'
os.environ['COGNITO_USERNAME'] ='sjain300@asu.edu'
os.environ['COGNITO_USER_POOL_ID'] ='us-east-1_7Rq8X1q2q'
os.environ['COGNITO_CLIENT_ID'] ='7v15em5a0iqvb3hn5r69cg3485'

def calculate_secret_hash(username, client_id, client_secret):
    message = username + client_id
    dig = hmac.new(
        key=client_secret.encode('utf-8'),
        msg=message.encode('utf-8'),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(dig).decode()

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
                'SECRET_HASH': secret_hash  # Use the calculated secret hash here
            }
        )
        
        return {
            "status": 200,
            "access_token": response['AuthenticationResult']['AccessToken']
        }
        
    except ClientError as e:
        return {
            "status": 500,
            "message": str(e)
        }

litellm_proxy_endpoint = os.environ.get(
    "litellm_proxy_endpoint",
    "https://api-llm.ctl-gait.clientlabsaft.com")
temperature = 0
max_tokens = 4096
bearer_token=get_cognito_token()['access_token']
 
url = f"{litellm_proxy_endpoint}/model/info"

payload = ""
headers = {
  'Authorization': f'Bearer {bearer_token}',
  'x-api-key': 'Bearer sk-_UCJBhDuE_YPLi4B4gxASQ' 
}

response = requests.request("GET", url, headers=headers, data=payload, verify=True)#'cacert.pem')

list_of_models = []
for models in response.json()['data']:
    list_of_models.append(models['model_name'])

# print(list_of_models)

chat = ChatOpenAI(
    openai_api_base=litellm_proxy_endpoint, # set openai_api_base to the LiteLLM Proxy
    model = 'Azure OpenAI GPT-4o (External)',
    default_headers={'x-api-key': 'Bearer sk-isbnmolh-BLL-CYaTZzp6w'},
    temperature=temperature,
    api_key=bearer_token,
    streaming=False,
    #max_tokens=max_tokens,
    user=bearer_token
)

# Prepare the message for the model
def prepare_summary_message(messages):
    message = f"""
        Summarize me the data analysis assistant report from below all the sub-reports.
        {messages}
        Please provide a structured data profiling report.
    """
    return message

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
                'SECRET_HASH': secret_hash  # Use the calculated secret hash here
            }
        )
        
        return {
            "status": 200,
            "access_token": response['AuthenticationResult']['AccessToken']
        }
        
    except ClientError as e:
        return {
            "status": 500,
            "message": str(e)
        }

# Helper function to chunk large CSV data
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

# Prepare the message for the model
def prepare_message(csv_data):
    return f"""
You are a data analysis assistant. The following is a CSV dataset. Please perform a data profiling analysis on it.
Provide insights about the data types of each column, identify any missing values (and their percentage), and provide any statistical summaries you can, such as mean, median, mode, min, max for numerical columns.

CSV data:
{csv_data}

Please provide a structured data profiling report.
    """

# Prepare the messages for the LLM model
def create_messages(csv_data):
    message = prepare_message(csv_data)
    return [
        SystemMessage(content="You are a data analysis assistant. Analyze the following CSV data and provide a detailed data profiling report."),
        HumanMessage(content=message)
    ]

# Call the LiteLLM API
def call_lite_llm_api(csv_data, bearer_token):
    url = os.environ.get("litellm_proxy_endpoint", "https://api-llm.ctl-gait.clientlabsaft.com") + "/model/info"
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'x-api-key': os.environ.get('LITELLM_API_KEY')
    }
    
    response = requests.request("GET", url, headers=headers, data="")
    return response.json()

# Function to display profiling report
def display_profiling_report(response):
    content = response.content
    if not content:
        print("No content found in the response.")
        return

    content = content.replace("#", "").replace("*", "")
    print("Data Profiling Report:\n")

    sections = content.split("\n\n")
    for section in sections:
        print(section.strip())
    print("End of Profiling Report.\n")

# Lambda Handler: Entry point for the Lambda function
def lambda_handler(event, context):
    # Retrieve the CSV data from the event or S3 bucket
    file_path = event.get('file_path')  # or event['csv_data']
    
    # Read CSV data (or retrieve from S3, etc.)
    csv_data = read_csv_to_string(file_path)
    
    # Get the Cognito bearer token
    bearer_token = get_cognito_token()['access_token']
    
    # Chunk the data if needed
    chunk_data = chunk_csv_data(csv_data)

    # Call LiteLLM API to analyze CSV data
    responses = []
    for chunk in chunk_data:
        messages = create_messages(chunk)
        
        # Use the `chat` function to send the request to the model
        try:
            response = chat(messages)
            responses.append(response)
        except Exception as e:
            print(f"Error: {e}")
    
    # Merge the responses if necessary
    merged_response = merge_responses(responses)
    display_profiling_report(chat(prepare_summary_message(merged_response)))

# Read CSV from S3 (if file_path is an S3 path)
def read_csv_to_string(file_path):
    if file_path.startswith('s3://'):
        s3_client = boto3.client('s3')
        bucket_name, key = file_path[5:].split('/', 1)
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        return obj['Body'].read().decode('utf-8')
    else:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()

# Merge responses from multiple chunks
def merge_responses(responses):
    merged_response = ""
    for response in responses:
        merged_response += response.content
    return merged_response
