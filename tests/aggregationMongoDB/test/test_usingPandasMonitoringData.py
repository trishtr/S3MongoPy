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
    
    data = []
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
    
                key = obj['Key']
                
                obj_response = s3_client.get_object(Bucket=bucket_name, Key=key)
                body = obj_response['Body'].read().decode('utf-8')

                if 'CENSUS_EVENT_FORECAST' in body: 
                    
                    payload = json.loads(body).get('payload')
                
                    df = pd.json_normalize(payload, 'census_data')
                    # print(df)

                    # 74b_ domainId: 4
                    filtered_df = df[(df['unit_code'] == '4')]

                    # print(filtered_df)
                    rows_with_zero_04 = len(filtered_df[(filtered_df['predicted_census'] == 0)])
                    # print(f'UnitCode : eff8d74b, dommainId : 4, with {rows_with_zero_04} rows with predicted_census = 0')   

                    # ff8d751_ domainId: 5 
                    filtered_df_05 = df[(df['unit_code'] == '5')]

                    # print(filtered_df_05)
                    rows_with_zero_05 = len(filtered_df_05[(filtered_df_05['predicted_census'] == 0)])
                    # print(f'UnitCode : eff8d74b, dommainId : 4, with {rows_with_zero_05} rows with predicted_census = 0')


                    # 9deff8d751_ domainId: 15 _
                    filtered_df_15 = df[(df['unit_code'] == '15')].reset_index(drop = True)

                    print(filtered_df_15)
                    rows_with_zero_15 = len(filtered_df_15[(filtered_df_15['predicted_census'] == 0)])
                    print(f'UnitCode : 5709deff8d751, dommainId : 15, with {rows_with_zero_15} rows with predicted_census = 0')

                    if (rows_with_zero_15 !=0):
                       
                        zero_indices(filtered_df_15)
                        
    



def zero_indices(filtered_df):
    zero_indices = filtered_df.index[filtered_df['predicted_census'] == 0].tolist()

    # Define the number of rows to print before and after
    rows_to_print = 1
    print(zero_indices)
    

    for idx in zero_indices:
    
        start_idx = max(0, idx - rows_to_print)
        print(f'row before 0 : {filtered_df.iloc[start_idx]}')

        predicted_0_idx = filtered_df.iloc[idx]
        print(f'row with predicted 0 : {predicted_0_idx}')

        end_idx = min(len(filtered_df), idx + rows_to_print + 1)
        print(f'row after 0: {filtered_df.iloc[end_idx]}')
    
        


