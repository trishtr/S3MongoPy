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
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
        self.s3_client = None

    def create_s3_session(self):

        try: 
            stage = os.environ.get('STAGE',None)
            if stage == 'qa':
                session  = boto3.Session(region_name=self.aws_regionName) 
                
            else:
                session = boto3.Session(
                aws_access_key_id= self.aws_accessKey,
                aws_secret_access_key= self.aws_secretKey,
                aws_session_token = self.aws_sessionToken,
                region_name= self.aws_regionName
            )   
               
            self.s3_client = session.client('s3')
            logger.info("S3 session created successfully.")
        except ConnectionError as e:
            self.logger.error(e)
            self.s3_client = None  # Explicitly set to None to avoid using uninitialized client
            raise



    def list_objects(self, bucket_name, prefix):
        if self.s3_client is None:
            self.logger.error("S3 client is not initialized. Call `create_s3_session` first.")
            raise RuntimeError("S3 client not initialized.")  # Raise error to avoid using NoneType

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
                        logger.info('create the key list with long-term and short-term forecast')
        if not key_list:
            logger.error(f'No long-term or short-term files are found in bucket {bucket_name} with {prefix}')
            return
            
        return key_list

    def read_object(self, bucket_name, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            logger.info(f"Successfully read object '{key}' from bucket '{bucket_name}'.")

            if response is None:
                # Log an error message if the response is None
                logger.error(f"Failed to retrieve object '{key}' from bucket '{bucket_name}'.")
                return None  # Return None to indicate failure

            # Read and return the object content as a decoded string
            return response["Body"].read().decode("utf-8")

        except Exception as e:
            # Log any exceptions encountered during the read operation
            self.logger.error(f"Error reading object '{key}' from bucket '{bucket_name}': {e}")
            raise  # Re-raise the exception to indicate a critical error
      

    def write_object(self, bucket_name, key, data):
        try:
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=data,
                ContentType="application/json",
            )
            logger.info(f"Writing message to bucket {bucket_name} with key {key}")
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")

    def create_new_key(self, key):
        # For real run-time
        # client_id = key.split('/')[0]

        # TODO: delete client_id = 'TestClient' when real time running 
        # For testing purpose
        client_id = 'TestClient'  

        event_id = str(uuid.uuid4())
        year = key.split('/')[2]
        month = key.split('/')[3]
        day  = key.split('/')[4]
        input_date_str = f"{year}/{month}/{day}"
        input_datetime = datetime.strptime(input_date_str, '%Y/%m/%d')
        output_date_str = input_datetime.strftime('%Y/%-m/%-d')
        new_s3_key = f"{client_id}/{output_date_str}/{event_id}"
        return new_s3_key
    
    def create_meta_data(self,key):
        client_id = key.split('/')[0]
        location_id = key.split('/')[1]
        event_id = str(uuid.uuid4())
        # Get the current timestamp in UTC
        current_time = datetime.now(timezone.utc)
        current_time_iso = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
        event_time_ms = int(current_time.timestamp() * 1000)

        if 'Short' in key:
                data = {'event_type':'CENSUS_INFERENCE_EVENT_PROJECT','version':'1.0',
                        'event_id': event_id,
                        'event_time_ms':event_time_ms, 'event_time_iso': current_time_iso,
                        'location_id':location_id,'event_source':'ML_LAMBDA_CENSUS',
                        'client_id': client_id,'timestamp' : current_time_iso ,
                        'payload':[{'blockSize': 1, 'census_data': []}]}
                logger.info(f'create new-key for short term predict with event_id {event_id}')
        elif 'Long' in key:
                data = {'event_type':'CENSUS_INFERENCE_EVENT_FORECAST','version':'1.0',
                        'event_id': event_id,
                        'event_time_ms':event_time_ms, 'event_time_iso': current_time_iso,
                        'location_id':location_id,'event_source':'ML_LAMBDA_CENSUS',
                        'client_id': client_id,'timestamp' : current_time_iso ,
                        'payload':[{'blockSize': 4, 'census_data': []}]}
                logger.info(f'create new-key for long term predict with event_id {event_id}')
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
            self.logger.error(f"Message delivery failed: {err,message}")
        else:
            self.logger.info("Publishing the message")


class CsvToJsonConverter:
    def __init__(self, s3_handler, mongo_handler, kafka_handler, logger):
        self.s3_handler = s3_handler
        self.mongo_handler = mongo_handler
        self.kafka_handler = kafka_handler
        self.logger = logger

    def convert(self, bucket_name, partial_prefix, target_bucket, target_topic):

        if self.s3_handler.s3_client is None:
            self.s3_handler.create_s3_session()
        # List all objects in the S3 bucket with the given prefix
        key_list = self.s3_handler.list_objects(bucket_name, partial_prefix)

        if not key_list:
            logger.error("No CSV files found in the specified bucket and prefix.")
            return

        for key in key_list:

            client_id = key.split('/')[0]
            location_id = key.split('/')[1]
            

            # Load unit taxonomy and tenant configuration
           
            unit_code_unit_id_dict = self.mongo_handler.load_unit_taxonomy(
                client_id, location_id
            )
            logger.info('successfully retrieved unit_id and unit_code mapping from mongoDB')

            tz_mapping = self.mongo_handler.load_tenant_config(client_id, location_id)
            logger.info('successfully retrieved timezone mapping from mongoDB')

            data = self.s3_handler.create_meta_data(key)

            # Read the CSV content from S3
            csv_content = self.s3_handler.read_object(bucket_name, key)

            # Parse the CSV content into a list of dictionaries
            csv_file = StringIO(csv_content)
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                unit_id = row["unitId"]
                if unit_id not in unit_code_unit_id_dict:
                    logger.error(f"Unit ID {unit_id} not found in unit_code_unit_id_dict.")
                    continue  # Skip the current iteration if the key doesn't exist

                local_date_time = datetime.strptime(
                    row["localDateTime"], "%Y-%m-%d %H:%M:%S"
                )

                predicted_census = float(row["predictedCensus"])
                if predicted_census < 0:
                    predicted_census = 0.0

                t_zone = pytz.timezone(tz_mapping[f"{client_id}_{location_id}"])
                localized_date_time = t_zone.localize(local_date_time)

                forecast_datetime = (
                    localized_date_time.strftime('%Y-%m-%dT%H:%M:%S.000%z')[:-2]
                    + ":"
                    + localized_date_time.strftime('%Y-%m-%dT%H:%M:%S.000%z')[-2:]
                )

                data["payload"][0]["census_data"].append(
                    {
                        "unit_code": unit_code_unit_id_dict.get(unit_id, None),
                        "forecast_datetime": forecast_datetime,
                        "predicted_census": predicted_census,
                    }
                )
            logger.info(f'successfully converted csv file with {key} to Json format')

            # Convert the data dictionary to JSON
            json_data = json.dumps(data, separators=(",", ":"))

            # Define the new S3 key for the converted data
           
            new_s3_key = self.s3_handler.create_new_key(key)

            # Write the JSON data to the target S3 bucket
            self.s3_handler.write_object(target_bucket, new_s3_key, json_data)

            # Log the output
            logger.info(f"Processed and wrote JSON data - with key {new_s3_key} to {target_bucket} bucket")

            self.kafka_handler.produce(target_topic, json_data)

            # log the output
            logger.info(f"Processed and published the Json data with key {new_s3_key} to {target_topic} topic")
            


# Create a logger instance
logger = Logger("CsvToJsonConverterLogger").get_logger()

# Create S3 session and handlers
accessKey = ''
secretKey = ''
sessionToken = ''

regionName = 'us-east-1'
s3_handler = S3Handler(accessKey, secretKey, sessionToken, regionName,logger)



# Create MongoDB handler with your MongoDB connection string
mongo_connection_string = ''

mongo_handler = MongoHandler(mongo_connection_string, logger)

# Create Kafka handler 
kafka_broker = ""
kafka_handler = KafkaProducerHandler(kafka_broker, logger)

# Create an instance of the CSV to JSON converter and run the conversion
converter = CsvToJsonConverter(s3_handler, mongo_handler, kafka_handler, logger)
converter.convert(
    bucket_name="uat-s3-ml-inference-output",
    partial_prefix="A/A/2024/04/14/",
    target_bucket="uat-s3-ml-publisher-output",
    target_topic="qa-testing",
)


    
        

