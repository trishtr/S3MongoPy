import boto3
import csv


def create_manifest_csv():
    
    
    s3 = boto3.Session(
        aws_access_key_id= '',
        aws_secret_access_key= "",
        aws_session_token = "",
        region_name= "us-east-1"
    )
    s3_client = s3.client('s3')
    
    # List all objects in the S3 bucket

    bucket_name = ""
    partial_prefix =''

    object_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                object_keys.append(key)

    manifest_file_key = 'TestClient11.csv'
    
    with open(manifest_file_key, 'w', newline='') as csvfile:
        fieldnames = ['SourceBucket', 'SourceKey']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for object_key in object_keys:
            writer.writerow({'SourceBucket': bucket_name, 'SourceKey': object_key})

    # Upload the manifest CSV file to S3
    # s3_client.upload_file(manifest_file_key, bucket_name, manifest_file_key)



if __name__ == "__main__":
    create_manifest_csv()
    

