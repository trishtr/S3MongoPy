import boto3
import os
from datetime import datetime, timezone


def download_all_objects_in_folder():
    

    
    s3 = boto3.Session(
        aws_access_key_id= '',
        aws_secret_access_key= "",
        aws_session_token = "",
        region_name= "us-east-1"
    )
    s3_client = s3.client('s3')
    
    # List all objects in the S3 bucket

    bucket_name = "scm-oi-prod-useast1-s3-raw-hl7"
    partial_prefix = 'ABC/2024/1/26/'
    local_directory = os.path.expanduser('~/Desktop')

    target_day = datetime(2024, 2, 20, tzinfo=timezone.utc).replace(tzinfo=None)
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                last_modified = obj['LastModified'].replace(tzinfo=None)

                # Extract the directory path of the object
                dir_name = os.path.dirname(key)

                _, _, month, day = dir_name.split('/')
                day_folder = f"{month}> {day}>"

                # Create the corresponding local directory if it doesn't exist
                os.makedirs(os.path.join(local_directory, day_folder), exist_ok=True)
                # Download the object and save it into the corresponding local directory
                local_file_path = os.path.join(local_directory, day_folder, os.path.basename(key))
                if last_modified < target_day:

                    s3_client.download_file(bucket_name, key, local_file_path)

if __name__ == "__main__":
    download_all_objects_in_folder()
    

