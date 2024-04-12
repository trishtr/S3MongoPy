from datetime import datetime, timezone
import pandas as pd
import json
import os
from io import StringIO
import csv
from pathlib import Path
import uuid
import logging
from confluent_kafka import Producer
import certifi
import pymongo
import boto3


def publisher_output():

    bucket_name = "s3-ml-inference-output"
    partial_prefix = 'a/b/2024/04/08/'

    target_bucket = "s3-ml-publisher-output"

    # target_topic = 'forecast'
    target_topic = 'raw-'

    csv_convert_json(bucket_name, partial_prefix, target_bucket)
     
    

def create_s3_session():
    
 
    aws_accessKey = ''
    aws_secretKey = ''
    aws_sessionToken = ''

    aws_regionName = ''

    try: 
        stage = os.environ.get('STAGE',None)
        if stage == 'qa':
            session  = boto3.Session()
            return session
            
        else:
            session = boto3.Session(
            aws_access_key_id= aws_accessKey,
            aws_secret_access_key= aws_secretKey,
            aws_session_token = aws_sessionToken,
            region_name= aws_regionName
        )
            return session
    except ConnectionError as e:
        print(e)

def csv_convert_json(bucketName, partial_prefix, target_bucket, target_topic):

    session = create_s3_session()

    s3_client = session.client('s3')

    key_lst = []
    json_data_list = []

    
    # event_id = str(uuid.uuid4())
   
    current_time = datetime.now(timezone.utc)
    current_time_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
    event_time_ms = int(current_time.timestamp() * 1000)
    # unit_code_unit_id_dict = loadMongoDB()
    
    # Paginate through objects in the bucket with the specified prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucketName, Prefix= partial_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if 'preferred' in key and ('Short' in key or 'Long' in key): 
                    # print(key)
                    key_lst.append(key)

                else:
                    print('no preferred files found to process')



                
    print(key_lst)
                
    for key in key_lst:

        # Use this code block to test
        client_id = 'TestClient'
        location_id = 'TestLocation'
        unit_code_unit_id_dict = loadMongoDB('a', 'b')

        # Use this code block for running with a
        # client_id = key.split('/')[0]
        # location_id = key.split('/')[1]
        # unit_code_unit_id_dict = loadMongoDB(client_id, location_id)

        event_id = str(uuid.uuid4())
        year = key.split('/')[2]
        month = key.split('/')[3]
        day  = key.split('/')[4]
        input_date_str = f"{year}/{month}/{day}"
        input_datetime = datetime.strptime(input_date_str, '%Y/%m/%d')
        output_date_str = input_datetime.strftime('%Y/%-m/%-d')

        new_s3_key = f"{client_id}/{output_date_str}/{event_id}"
       


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
                    'location_id':location_id,'event_source':'ML_LAMBDA_CENSUS',
                    'client_id': client_id,'timestamp' : current_time_iso ,
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
                "predicted_census": float(predicted_census)
                    
            })

           
        json_data = json.dumps(data)

        with open(f'{event_id}.json', 'w')as json_file:
            json_file.write(json_data)

        try:
         
            print(f"Writing message to bucket {target_bucket} with key {new_s3_key}")
        
            s3_client.put_object(
                Bucket= target_bucket,
                Key=new_s3_key,
                Body= json_data,
                ContentType="application/json"
            )
        except Exception as e:
            print(f"Error: {e}")

        send_message_to_msk(target_topic,json_data)
    #     json_data_list.append(json_data)
       
    # return json_data_list

def loadMongoDB(clientId, locationId):

    mongo_connectionString = ''
    try: 
        ca = certifi.where()
        client = pymongo.MongoClient(mongo_connectionString, serverSelectionTimeoutMS=5000,tlsCAFile=ca)
        databaseName = 'scm-data-lake'
        dbname = client.get_database(databaseName)
        print("Create connecting session")

        colName = 'unitTaxonomy'
    
        unitTaxonomy = dbname.get_collection(colName)
        # tenantConfig = dbname.get_collection('tenantConfiguration')

        query = {'client.clientId': clientId, 'client.locationId': locationId}
        docs = unitTaxonomy.find(query)
        unitId_unitCode_map = {}

        for doc in docs:
            unitId = str(doc.get('_id'))
            domainIds = doc.get('internalIds')[0].get('domainId')
            unitId_unitCode_map[unitId] = domainIds
        print(unitId_unitCode_map)
        return unitId_unitCode_map
    
    except ConnectionError as e:
        print(f"Connection error: {e}")
        raise ConnectionError("Failed to establish a MongoDB connection")
    




def send_message_to_msk(target_topic,msg):
    kafka_broker = ""
    
    producer = Producer({
    'bootstrap.servers': kafka_broker,
    'socket.timeout.ms': 1000,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000,
    'security.protocol' : 'SSL',
    'ssl.key.location' : 'key2024.pem', #This is the private_key.txt from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'ssl.certificate.location': 'cert2024.pem', #this is the  Certificate.txt and certificate_chain.txt files merged together - the certificate_chain goes on bottom. from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'ssl.ca.location' : 'CARoot.pem', #This is the Amazon Root Public CARoots from here: https://www.amazontrust.com/repository/
    'ssl.key.password' : '123456', # This is the passphrase  from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'delivery.report.only.error' : True
})
    
    # msg = {'msg':'testing_qa_producer'}
    try:
        producer.produce(target_topic, value=msg,
        callback=lambda err, original_msg=msg: roll_back(err, original_msg),)  
    except Exception as e:
        print(e)
    finally:
        producer.flush()

def roll_back(err, original_msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered successfully: {original_msg}")
    
 

if __name__=='__main__':
    
    publisher_output()
