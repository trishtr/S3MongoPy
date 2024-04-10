import boto3
import os
from datetime import datetime, timezone
import pandas as pd
import json
from io import StringIO
import csv
from pathlib import Path
import uuid
import logging


def test_publisherOutput_Transform(s3_session,getDatabaseName):
 
    # Read inference-output-csv file: 
    # Read csv file from s3-ml-inference-output, convert to json file with predefined meta data, send to target bucket in S3

    bucket_name = "s3-ml-inference-output"
    partial_prefix = 'A/AS/2024/04/08/'

    target_bucket = "s3-ml-publisher-output"

                 
    json_data_list = csv_convert_json(s3_session,bucket_name, partial_prefix, getDatabaseName)
     
    print(len(json_data_list))
    


    for i, json_data in enumerate(json_data_list):
        with open(f'file_{i+1}.json', 'w')as json_file:
            json_file.write(json_data)

        print('write to S3')
        msg = json.loads(json_data)
        write_to_s3(s3_session, msg,target_bucket)
                

def csv_convert_json(s3_session,bucketName,partial_prefix,getDatabaseName):
    s3  = s3_session    
    s3_client = s3.client('s3')
    key_lst = []
    json_data_list = []

    
    event_id = str(uuid.uuid4())
    client_id = 'TestClient'
    location_id = 'TestLocation'
    current_time = datetime.now(timezone.utc)
    current_time_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
    event_time_ms = int(current_time.timestamp() * 1000)
    unit_code_unit_id_dict = loadMongoDB(getDatabaseName)
    
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucketName, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if 'preferred' in key and ('Short' in key or 'Long' in key): 
                    # print(key)
                    key_lst.append(key)
    print(key_lst)
    for key in key_lst:

        if 'Short' in key:
            data = {'event_type':'CENSUS_INFERENCE_EVENT_PROJECT','version':'1.0',
                    'event_id': event_id,
                    'event_time_ms':event_time_ms, 'event_time_iso': current_time_iso,
                    'location_id':location_id,'event_source':'ML_LAMBDA_CENSUS',
                    'client_id': client_id,'timestamp' : current_time_iso ,
                    'payload':[{'blockSize': 1, 'census_data': []}]}
        elif 'Long' in key:
            data = {'event_type':'CENSUS_INFERENCE_EVENT_FORECAST','version':'1.0',
                    'event_id': event_id,
                    'event_time_ms':event_time_ms, 'event_time_iso': current_time_iso,
                    'location_id':'APPRHS','event_source':'ML_LAMBDA_CENSUS',
                    'client_id': 'APPRHS','timestamp' : current_time_iso ,
                    'payload':[{'blockSize': 4, 'census_data': []}]}

       
        
        response = s3_client.get_object(Bucket=bucketName, Key=key)
        csv_data = response['Body'].read().decode('utf-8')
                   
        csv_file = StringIO(csv_data)
                    
        csv_reader = csv.DictReader(csv_file)          

        for row in csv_reader:
                            
            # print(row)
                    
            unit_id = row['unitId']
                    
            # print(unit_id)
            local_date_time = datetime.strptime(row['localDateTime'],'%Y-%m-%d %H:%M:%S' )
            predicted_census = float(row['predictedCensus'])
            if predicted_census < 0:
                predicted_census == 0.0
            forecast_datetime = local_date_time.strftime('%Y-%m-%dT%H:%M:%S.000-04:00')

            data["payload"][0]['census_data'].append({
                    
                  
                "unit_code": unit_code_unit_id_dict[unit_id],
                "forecast_datetime": forecast_datetime,
                "predicted_census": round(predicted_census,1)
                    
            })

           
        json_data = json.dumps(data)


        json_data_list.append(json_data)
       
    return json_data_list


def write_to_s3(s3_session, msg, target_bucket):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        s3  = s3_session    
        s3_client = s3.client('s3')
        key = f"{msg['client_id']}/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{msg['event_id']}"
        # print(key)
        logger.info(f"Writing message to bucket {target_bucket} with key {key}")
        
        s3_client.put_object(
            Bucket= target_bucket,
            Key=key,
            Body= json.dumps(msg),
            ContentType="application/json"
        )
    except Exception as e:
        logger.error(f"Error: {e}")

def loadMongoDB(getDatabaseName):
    dbName = getDatabaseName
    
    colName = 'unitTaxonomy'
    
    unitTaxonomy = dbName.get_collection(colName)
    tenantConfig = dbName.get_collection('tenantConfiguration')

    query = {'client.clientId': 'APPRHS'}
    docs = unitTaxonomy.find(query)
    unitId_unitCode_map = {}

    for doc in docs:
        unitId = str(doc.get('_id'))
        domainIds = doc.get('internalIds')[0].get('domainId')
        unitId_unitCode_map[unitId] = domainIds
    print(unitId_unitCode_map)
    return unitId_unitCode_map
   
