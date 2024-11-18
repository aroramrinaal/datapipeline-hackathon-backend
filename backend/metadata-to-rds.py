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
import psycopg2

warnings.filterwarnings("ignore")

# Environment Variables
os.environ['COGNITO_CLIENT_SECRET'] = '11181idr8ho95i6k67v99p3go2sn72kri5j5qdg2lo4jl0uj9ri4'
os.environ['COGNITO_PASSWORD'] = 'HelloWorld#1'
os.environ['COGNITO_USERNAME'] = 'sjain300@asu.edu'
os.environ['COGNITO_USER_POOL_ID'] = 'us-east-1_7Rq8X1q2q'
os.environ['COGNITO_CLIENT_ID'] = '7v15em5a0iqvb3hn5r69cg3485'
os.environ['litellm_proxy_endpoint'] = 'https://api-llm.ctl-gait.clientlabsaft.com'
os.environ['RDS_HOST'] = "mydb.ctsqgmcogxc7.us-east-1.rds.amazonaws.com"
os.environ['DB_USER'] = "username"
os.environ['DB_PASSWORD'] = "password"
os.environ['DB_NAME'] = "mydb"

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
        print(f"Attempting to read file {file_key} from bucket {bucket_name}")
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        print(f"Successfully read file. First 100 chars: {content[:100]}")
        return content
    except Exception as e:
        print(f"Error reading file from S3: {str(e)}")
        raise Exception(f"Failed to read file from S3: {str(e)}")

        

def generate_metadata_via_llm(csv_data):
    """Generate metadata using LiteLLM."""
    lite_llm_url = "https://api-llm.ctl-gait.clientlabsaft.com/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": "Bearer sk-_UCJBhDuE_YPLi4B4gxASQ",
        "Authorization": f"Bearer {get_cognito_token()}"
    }
    prompt = f"""
    Analyze the following CSV data and return ONLY a JSON object containing metadata about the dataset. 
    Do not include any explanatory text before or after the JSON.
    The JSON should follow this exact structure:

    {{
        "dataset_overview": {{
            "total_rows": <number>,
            "total_columns": <number>,
            "total_missing_values": <number>,
            "total_unique_values": <number>
        }},
        "columns": {{
            "<column_name>": {{
                "data_type": "<type>",
                "missing_values": <number>,
                "missing_percentage": <number>,
                "unique_values": <number>,
                "unique_percentage": <number>,
                "numerical_summary": {{
                    "min": <number>,
                    "max": <number>,
                    "mean": <number>,
                    "median": <number>,
                    "std_dev": <number>
                }},
                "flags": {{
                    "high_missing_percentage": <boolean>,
                    "potential_outliers": [<values>]
                }}
            }}
        }}
    }}

    CSV Data:
    {csv_data[:1000]}
    """
    
    payload = {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "model": "Azure OpenAI GPT-4o (External)",
        "temperature": 0
    }
    
    try:
        response = requests.post(lite_llm_url, headers=headers, json=payload, verify=True)
        response.raise_for_status()
        response_json = response.json()
        
        if 'choices' not in response_json or not response_json['choices']:
            raise ValueError("Invalid response format from LLM")
            
        metadata_content = response_json['choices'][0]['message']['content']
        
        # Clean the response - remove any markdown formatting if present
        metadata_content = metadata_content.replace('```json', '').replace('```', '').strip()
        
        # Parse the JSON
        metadata = json.loads(metadata_content)
        return metadata
            
    except Exception as e:
        print(f"Error in generate_metadata_via_llm: {str(e)}")
        raise

def save_metadata_to_s3(bucket_name, file_key, metadata):
    """Save metadata as a JSON file in S3."""
    try:
        s3_client = boto3.client('s3')
        # Create metadata file key by replacing .csv with _metadata.json
        metadata_file_key = file_key.replace(".csv", "_metadata.json")
        
        print(f"Original CSV file: {file_key}")
        print(f"Generated metadata file name: {metadata_file_key}")
        
        # Convert to formatted JSON string with nice indentation
        metadata_json = json.dumps(metadata, indent=2)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_file_key,
            Body=metadata_json,
            ContentType="application/json"
        )
        print(f"Successfully saved metadata to S3: {metadata_file_key}")
        return metadata_file_key
    except Exception as e:
        print(f"Error saving metadata: {str(e)}")
        raise Exception(f"Failed to save metadata to S3: {e}")

def connect_to_rds():
    """Connect to the PostgreSQL RDS instance."""
    try:
        connection = psycopg2.connect(
            host=os.environ.get('RDS_HOST'),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            database=os.environ.get('DB_NAME')
        )
        return connection
    except Exception as e:
        print(f"Error connecting to RDS: {e}")
        raise

def store_metadata_in_rds(metadata):
    """Store metadata in RDS PostgreSQL database."""
    try:
        connection = connect_to_rds()
        cursor = connection.cursor()

        # Create and populate dataset overview table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dataset_overview (
                id SERIAL PRIMARY KEY,
                total_rows INT,
                total_columns INT,
                total_missing_values INT,
                total_unique_values INT
            )
        """)

        cursor.execute("""
            INSERT INTO dataset_overview 
            (total_rows, total_columns, total_missing_values, total_unique_values)
            VALUES (%s, %s, %s, %s)
        """, (
            metadata['dataset_overview']['total_rows'],
            metadata['dataset_overview']['total_columns'],
            metadata['dataset_overview']['total_missing_values'],
            metadata['dataset_overview']['total_unique_values']
        ))

        # Create and populate columns metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS columns_metadata (
                id SERIAL PRIMARY KEY,
                column_name VARCHAR(255),
                data_type VARCHAR(255),
                missing_values INT,
                missing_percentage FLOAT,
                unique_values INT,
                unique_percentage FLOAT,
                numerical_summary_min FLOAT,
                numerical_summary_max FLOAT,
                numerical_summary_mean FLOAT,
                numerical_summary_median FLOAT,
                numerical_summary_std_dev FLOAT,
                high_missing_percentage_flag BOOLEAN,
                potential_outliers TEXT
            )
        """)

        # Insert column metadata
        for column_name, column_data in metadata['columns'].items():
            cursor.execute("""
                INSERT INTO columns_metadata (
                    column_name, data_type, missing_values, missing_percentage,
                    unique_values, unique_percentage, numerical_summary_min,
                    numerical_summary_max, numerical_summary_mean, numerical_summary_median,
                    numerical_summary_std_dev, high_missing_percentage_flag, potential_outliers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                column_name,
                column_data['data_type'],
                column_data['missing_values'],
                column_data['missing_percentage'],
                column_data['unique_values'],
                column_data['unique_percentage'],
                column_data['numerical_summary'].get('min'),
                column_data['numerical_summary'].get('max'),
                column_data['numerical_summary'].get('mean'),
                column_data['numerical_summary'].get('median'),
                column_data['numerical_summary'].get('std_dev'),
                column_data['flags']['high_missing_percentage'],
                json.dumps(column_data['flags']['potential_outliers'])
            ))

        connection.commit()
        cursor.close()
        connection.close()
        print("Successfully stored metadata in RDS")
        
    except Exception as e:
        print(f"Error storing metadata in RDS: {str(e)}")
        raise

def lambda_handler(event, context):
    """Main Lambda Handler for S3 triggers."""
    try:
        print("Received event:", json.dumps(event))
        
        # Extract bucket and file information from S3 event
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        file_key = s3_event['object']['key']
        
        print(f"Processing S3 event - Bucket: {bucket_name}, File: {file_key}")
        
        # Only process CSV files
        if not file_key.lower().endswith('.csv'):
            print(f"Skipping non-CSV file: {file_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Skipped non-CSV file'
                })
            }
        
        # Read CSV data
        print("Starting to read CSV data")
        csv_data = read_csv_from_s3(bucket_name, file_key)
        print("Successfully read CSV data")
        
        # Generate metadata
        print("Starting metadata generation")
        metadata = generate_metadata_via_llm(csv_data)
        print("Successfully generated metadata")
        
        # Save metadata to S3
        print("Starting to save metadata to S3")
        metadata_file_key = save_metadata_to_s3(bucket_name, file_key, metadata)
        print(f"Successfully saved metadata to: {metadata_file_key}")
        
        # Store metadata in RDS
        print("Starting to store metadata in RDS")
        store_metadata_in_rds(metadata)
        print("Successfully stored metadata in RDS")
        
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully generated and saved metadata to S3 and RDS',
                'metadata_file': metadata_file_key,
                'metadata': metadata
            })
        }
        print(f"Lambda execution completed successfully: {json.dumps(response)}")
        return response
        
    except Exception as e:
        error_response = {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to process request: {str(e)}'
            })
        }
        print(f"Error occurred: {str(e)}")
        print(f"Error response: {json.dumps(error_response)}")
        import traceback
        traceback.print_exc()
        return error_response

