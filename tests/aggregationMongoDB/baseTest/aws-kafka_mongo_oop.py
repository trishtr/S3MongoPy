import os
import logging
import boto3
import pymongo
import pytz
import certifi
import uuid
import json
import csv
import datetime
from io import StringIO
from confluent_kafka import Producer
from datetime import datetime, timezone


class Logger:
    def __init__(self, name, level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        if not self.logger.hasHandlers():
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message%s")
            stream_handler.setFormatter(formatter)
            self.logger.addHandler(stream_handler)
        self.logger.setLevel(level)

    def get_logger(self):
        return self.logger
    
class S3Handler:
    def __init__(self, accessKey, secretKey, sessionToken, regionName, logger):
        self.logger = logger
        self.aws_accessKey = accessKey
        self.aws_secretKey = secretKey
        self.aws_sessionToken = sessionToken
        self.aws_regionName = regionName

    def create_s3_session(self):

        try: 
            stage = os.environ.get('STAGE',None)
            if stage == 'qa':
                session  = boto3.Session()
                s3_client = session.client('s3')
                return s3_client
                
            else:
                session = boto3.Session(
                aws_access_key_id= self.aws_accessKey,
                aws_secret_access_key= self.aws_secretKey,
                aws_session_token = self.aws_sessionToken,
                region_name= self.aws_regionName
            )   
                s3_client = session.client('s3')
                return s3_client
        except ConnectionError as e:
            self.logger.error(e)



    def list_objects(self, bucket_name, prefix):
        
        key_list = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if (
                        "preferred" in key
                        and ("Short" in key or "Long" in key)
                        and key.endswith(".csv")
                    ):
                        key_list.append(key)
                    else:
                        self.logger.error("No preferred files found to process.")
        return key_list

    def read_object(self, bucket_name, key):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read().decode("utf-8")

    def write_object(self, bucket_name, key, data):
        try:
            self.logger.info(f"Writing message to bucket {bucket_name} with key {key}")
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ContentType="application/json",
            )
        except Exception as e:
            self.logger.error(f"Error writing to S3: {e}")

    def create_new_key(self, key):
        logger.info("creating new s3 key")
        client_id = self.key.split('/')[0]
        event_id = str(uuid.uuid4())
        year = self.key.split('/')[2]
        month = key.split('/')[3]
        day  = key.split('/')[4]
        input_date_str = f"{year}/{month}/{day}"
        input_datetime = datetime.strptime(input_date_str, '%Y/%m/%d')
        output_date_str = input_datetime.strftime('%Y/%-m/%-d')
        new_s3_key = f"{client_id}/{output_date_str}/{event_id}"
        return new_s3_key
    
    def create_meta_data(self,key):
        client_id = self.key.split('/')[0]
        location_id = self.key.split('/')[1]
        event_id = str(uuid.uuid4())
        # Get the current timestamp in UTC
        current_time = datetime.datetime.now(datetime.timezone.utc)
        current_time_iso = current_time.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        event_time_ms = int(current_time.timestamp() * 1000)

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
        return data




class MongoHandler:
    def __init__(self, mongo_connection_string, logger):
        self.mongo_connection_string = mongo_connection_string
        self.logger = logger

    def get_database(self, database_name):
        try : 
            ca = certifi.where()
            client = pymongo.MongoClient(
                self.mongo_connection_string, serverSelectionTimeoutMS=5000, tlsCAFile=ca
            )
            return client.get_database(database_name)
          
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise ConnectionError("Failed to establish a MongoDB connection")

    def load_unit_taxonomy(self, client_id, location_id):
        database_name = "scm-data-lake"
        db = self.get_database(database_name)
        collection = db.get_collection("unitTaxonomy")

        query = {"client.clientId": client_id, "client.locationId": location_id}
        docs = collection.find(query)

        unit_id_unit_code_map = {}
        for doc in docs:
            unit_id = str(doc.get("_id"))
            domain_id = doc.get("internalIds")[0].get("domainId")
            unit_id_unit_code_map[unit_id] = domain_id
        return unit_id_unit_code_map

    def load_tenant_config(self, client_id, location_id):
        database_name = "scm-data-lake"
        db = self.get_database(database_name)
        collection = db.get_collection("tenantConfiguration")

        query = {"clientId": client_id, "locationId": location_id}
        docs = collection.find(query)

        tz_mapping = {}
        for doc in docs:
            timezone = doc.get("timezone")
            tz_mapping[f"{client_id}_{location_id}"] = timezone
        return tz_mapping
    
class KafkaProducerHandler:
    def __init__(self, kafka_broker, logger):
        self.logger = logger
        self.kafka_broker = kafka_broker
        self.producer = Producer(
            {
                "bootstrap.servers": self.kafka_broker,
                "socket.timeout.ms": 1000,
                "api.version.request": "false",
                "broker.version.fallback": "0.9.0",
                "message.max.bytes": 1000000000,
                "security.protocol": "SSL",
                "ssl.key.location": "key2024.pem",
                "ssl.certificate.location": "cert2024.pem",
                "ssl.ca.location": "CARoot.pem",
                "ssl.key.password": "123456",
                "delivery.report.only.error": True,
            }
        )

    def produce(self, topic, message):
        try:
            self.producer.produce(
                topic,
                value=message,
                callback=self.delivery_report,
            )
        except Exception as e:
            self.logger.error(e)
        finally:
            self.producer.flush()

    def delivery_report(self, err, message):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.info("Message delivered successfully.")


class CsvToJsonConverter:
    def __init__(self, s3_handler, mongo_handler, kafka_handler, logger):
        self.s3_handler = s3_handler
        self.mongo_handler = mongo_handler
        self.kafka_handler = kafka_handler
        self.logger = logger

    def convert(self, bucket_name, partial_prefix, target_bucket, target_topic):

        # List all objects in the S3 bucket with the given prefix
        key_list = self.s3_handler.list_objects(bucket_name, partial_prefix)

        if not key_list:
            self.logger.error("No CSV files found in the specified bucket and prefix.")
            return

        for key in key_list:
            # Use this code block for real time running
            # client_id = key.split('/')[0]
            # location_id = key.split('/')[1]
            

            client_id = "TestClient"  # For testing purposes
            location_id = "TestLocation"  # For testing purposes

            # Load unit taxonomy and tenant configuration
            unit_code_unit_id_dict = self.mongo_handler.load_unit_taxonomy(
                client_id, location_id
            )
            tz_mapping = self.mongo_handler.load_tenant_config(client_id, location_id)
            
            data = self.s3_handler.create_meta_data(key)

            # Read the CSV content from S3
            csv_content = self.s3_handler.read_object(bucket_name, key)

            # Parse the CSV content into a list of dictionaries
            csv_file = StringIO(csv_content)
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                unit_id = row["unitId"]
                local_date_time = datetime.datetime.strptime(
                    row["localDateTime"], "%Y-%m-%d %H:%M:%S"
                )

                predicted_census = float(row["predictedCensus"])
                if predicted_census < 0:
                    predicted_census = 0.0

                t_zone = pytz.timezone(tz_mapping[f"{client_id}_{location_id}"])
                localized_date_time = t_zone.localize(local_date_time)

                forecast_datetime = (
                    localized_date_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2]
                    + ":"
                    + localized_date_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[-2:]
                )

                data["payload"][0]["census_data"].append(
                    {
                        "unit_code": unit_code_unit_id_dict[unit_id],
                        "forecast_datetime": forecast_datetime,
                        "predicted_census": predicted_census,
                    }
                )

            # Convert the data dictionary to JSON
            json_data = json.dumps(data, separators=(",", ":"))

            # Define the new S3 key for the converted data
            logger.info("creating new s3 key")
            new_s3_key = self.s3_handler.create_new_key(key)

            # Write the JSON data to the target S3 bucket
            self.s3_handler.write_object(target_bucket, new_s3_key, json_data)

            # Log the output
            self.logger.info(f"Processed and wrote JSON data to {new_s3_key}")

            self.kafka_handler.produce(target_topic, json_data)
            


# Create a logger instance
logger = Logger("CsvToJsonConverterLogger").get_logger()

# Create S3 session and handlers
accessKey = ''
secretKey = ''
sessionToken = ''
regionName = 'us-east-1'
s3_handler = S3Handler(accessKey, secretKey, sessionToken, regionName,logger)



# Create MongoDB handler with your MongoDB connection string
mongo_connection_string = "<MongoDB_Connection_String>"
mongo_handler = MongoHandler(mongo_connection_string, logger)

# Create Kafka handler 
bootstrap_server = ""
kafka_handler = KafkaProducerHandler(bootstrap_server, logger)

# Create an instance of the CSV to JSON converter and run the conversion
converter = CsvToJsonConverter(s3_handler, mongo_handler, kafka_handler, logger)
converter.convert(
    bucket_name="uat-s3-ml-inference-output",
    partial_prefix="APS/APS/2024/04/15/",
    target_bucket="uat-s3-ml-publisher-output",
    target_topic="qa-testing",
)


    
        

