import boto3
import os
import json
from botocore.exceptions import ClientError

BUCKET_NAME = 'uploadedcsvfiles'

def lambda_handler(event, context):
    # The file content is assumed to be passed in the 'body' of the event, base64-encoded
    file_content = event.get("body")  # Placeholder for testing
    file_name = event.get("queryStringParameters", {}).get("file_name", "uploaded_file.csv")

    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Upload the file to S3
        s3_client.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=file_content)
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': f"File '{file_name}' uploaded successfully to S3!"})
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }