import boto3
from botocore.config import Config
import os
import json
from botocore.exceptions import ClientError

BUCKET_NAME = 'uploadedcsvfiles'

def lambda_handler(event, context):

    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS'
            },
            'body': ''
        }

    # The file content is assumed to be passed in the 'body' of the event, base64-encoded
    file_content = event.get("body")
    file_name = event.get("queryStringParameters", {}).get("file_name", "")
    request_type = event.get("queryStringParameters", {}).get("request_type", "upload")
    
    # Generate unique filename if not provided
    if not file_name:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"uploaded_file_{timestamp}.csv"
    
    s3_client = boto3.client('s3',
        config=Config(
            s3={'addressing_style': 'path'},
            signature_version='s3v4',
            retries={'max_attempts': 3}
        )
    )
    
    try:
        # If requesting presigned URL (for large files)
        if request_type == "presigned":
            presigned_url = s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': BUCKET_NAME,
                    'Key': file_name,
                    'ContentType': 'text/csv'
                },
                ExpiresIn=3600  # URL expires in 1 hour
            )
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS'
                },
                'body': json.dumps({
                    'presigned_url': presigned_url,
                    'file_name': file_name
                })
            }
        
        # Direct upload for small files
        else:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=file_content,
                ContentType='text/csv'
            )
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS'
                },
                'body': json.dumps({'message': f"File '{file_name}' uploaded successfully to S3!"})
            }
            
    except ClientError as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS'
            },
            'body': json.dumps({'error': str(e)})
        }