import boto3
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



def publish_output():

    bucket_name = "s3-ml-inference-output"
    partial_prefix = 'A/A/2024/04/08/'

    target_bucket = "s3-ml-publisher-output"

    target_topic = 'sw-census-forecast'

    session = create_s3_session()

    json_data_list = csv_convert_json(session,bucket_name, partial_prefix)
     
    # print(len(json_data_list))

    for i, json_data in enumerate(json_data_list):
        with open(f'file_{i+1}.json', 'w')as json_file:
            json_file.write(json_data)

        print('write to S3')
        
        msg = json.loads(json_data)

        write_to_s3(session, msg,target_bucket)
        send_message_to_msk(target_topic, json_data)


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

def csv_convert_json(bucketName, partial_prefix):

    session = create_s3_session()

    s3_client = session.client('s3')

    key_lst = []
    json_data_list = []

    
    event_id = str(uuid.uuid4())
    # client_id = 'TestClient'
    # location_id = 'TestLocation'
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
    # print(key_lst)

    for key in key_lst:

        client_id = key.split('/')[0]
        location_id = key.split('/')[1]
        unit_code_unit_id_dict = loadMongoDB(client_id, location_id)



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
                "predicted_census": round(predicted_census,1)
                    
            })

           
        json_data = json.dumps(data)


        json_data_list.append(json_data)
       
    return json_data_list


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


def write_to_s3(session, msg, target_bucket):
    
    session = create_s3_session()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
         
        s3_client = session.client('s3')
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

def send_message_to_msk(topic,msg):
    kafka_broker = "b-1.scmoiuatuseast1mskpri.lnd8df.c11.kafka.us-east-1.amazonaws.com:9094,b-2.scmoiuatuseast1mskpri.lnd8df.c11.kafka.us-east-1.amazonaws.com:9094,b-3.scmoiuatuseast1mskpri.lnd8df.c11.kafka.us-east-1.amazonaws.com:9094"
    
    producer = Producer({
    'bootstrap.servers': kafka_broker,
    'socket.timeout.ms': 10000,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000,
    'security.protocol' : 'SSL',
    'ssl.key.location' : 'key.pem', #Remember to create pem key and copy it to the folder, This is the private_key.txt from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'ssl.certificate.location': 'cert.pem', #this is the  Certificate.txt and certificate_chain.txt files merged together - the certificate_chain goes on bottom. from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'ssl.ca.location' : 'CARoot.pem', #This is the Amazon Root Public CARoots from here: https://www.amazontrust.com/repository/
    'ssl.key.password' : '123456sdfdfd', # This is the passphrase  from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'delivery.report.only.error' : True
})
    
    # msg = {'msg':'testing_qa_producer'}
    # topic = 'sw-census-forecast'
    try:
        producer.produce(topic, value=json.dumps(msg),
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