import boto3
import os
from datetime import datetime, timezone
import pandas as pd
import json
from io import StringIO
import csv
from pathlib import Path

def test_csv_json_convert(s3_session):
 
    # Read inference-output-csv file: 

    bucket_name = "-ml-inference-output"
    partial_prefix = 'A/A/2024/04/08/'
                 
    csv_files= read_csv_from_s3(s3_session,bucket_name, partial_prefix)
     
    json_data_list = csv_short_term_to_json(csv_files)
            
    for i, json_data in enumerate(json_data_list):
        with open(f'file_{i+1}.json', 'w')as json_file:
            json_file.write(json_data)




                

def read_csv_from_s3(s3_session,bucketName,partial_prefix):
    s3  = s3_session    
    s3_client = s3.client('s3')
    
    csv_files = []
    
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucketName, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
    
                key = obj['Key']
                if 'preferred' in key and 'Short' in key: 
                    print(key)
                    response = s3_client.get_object(Bucket=bucketName, Key=key)
                    csv_data = response['Body'].read().decode('utf-8')
                   
                    csv_file = StringIO(csv_data)
                    
                    csv_files.append(csv_file)
                   
    # print(csv_files)
    return csv_files

    
def csv_short_term_to_json(csv_files,getDatabaseName):
   
    for csv_file in csv_files:
        csv_reader = csv.DictReader(csv_file)
        data = {'payload':[{'blockSize': 1, 'census_data': []}]}
        json_data_list = []
        unit_code_unit_id_dict = readUnitTaxonomy(getDatabaseName)

        for row in csv_reader:
                
            # print(row)
        
            unit_id = row['unitId']
        
            # print(unit_id)
            local_date_time = datetime.strptime(row['localDateTime'],'%Y-%m-%d %H:%M:%S' )
            predicted_census = float(row['predictedCensus'])
            forecast_datetime = local_date_time.strftime('%Y-%m-%dT%H:%M:%S.000-04:00')

            data["payload"][0]['census_data'].append({
                    
                  
                        "unit_code": unit_code_unit_id_dict.get(unit_id),
                        "forecast_datetime": forecast_datetime,
                        "predicted_census": predicted_census
                    
                })

           
            json_data = json.dumps(data)


            json_data_list.append(json_data)
        print(json_data_list)
        return json_data_list
    
def readUnitTaxonomy(getDatabaseName):
    dbName = getDatabaseName
    
    colName = 'unitTaxonomy'
    
    unitTaxonomy = dbName.get_collection(colName)
   
    query = {'client.clientId': 'APPRHS'}
    docs = unitTaxonomy.find(query)
    unitId_unitCode_map = {}

    for doc in docs:
        unitId = str(doc.get('_id'))
        domainIds = doc.get('internalIds')[0].get('domainId')
        unitId_unitCode_map[unitId] = domainIds
    print(unitId_unitCode_map)
    return unitId_unitCode_map
   
