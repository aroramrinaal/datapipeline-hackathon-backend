def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'your-s3-bucket-name'
    file_content = event['file_content']  # Assuming file content is passed in the event
    file_name = event['file_name']

    # Upload file to S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
    return {'statusCode': 200, 'body': 'File uploaded successfully!'}