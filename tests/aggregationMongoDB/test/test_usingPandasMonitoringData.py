import boto3
import os
from datetime import datetime, timezone
import pandas as pd
import json


def test_publisherOutput(s3_session):
    s3  = s3_session
    
    s3_client = s3.client('s3')
    
    # List all objects in the S3 bucket

    bucket_name = "publisher-output"
    partial_prefix = 'A/2024/4/1/'
    
   
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
    
                key = obj['Key']
                
                obj_response = s3_client.get_object(Bucket=bucket_name, Key=key)
                body = obj_response['Body'].read().decode('utf-8')

                if 'CENSUS_FORECAST' in body: 
                    
                    payload = json.loads(body).get('payload')
                
                    df = pd.json_normalize(payload, 'census_data')
                    # print(df)

                    # 6230e8d02fd5709deff8d74b_ domainId: 4_ARWH Nursing - 3BT ARWH: 2W Med Surg
                    filtered_df_04 = df[(df['unit_code'] == '4')]

                    print(filtered_df_04)
                    rows_with_zero_04 = len(filtered_df_04[(filtered_df_04['predicted_census'] == 0)])
                    print(f'UnitCode : 74b, dommainId : 4, with {rows_with_zero_04} rows with predicted_census = 0')