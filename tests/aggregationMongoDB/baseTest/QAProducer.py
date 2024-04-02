import boto3
import json
from confluent_kafka import *
import pprint
import os
import traceback
import logging
from datetime import datetime
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
             format='%(asctime)s %(message)s',
             level=logging.INFO,
             datefmt='%Y-%m-%d %H:%M:%S')

## JSON of keys with the root node being "Contents"
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
# s3_bucket = os.environ["RAW_BUCKET"]
KAFKA_BROKER = os.environ["MSK_BROKER_URL_TLS"]
KAFKA_TOPIC = 'raw-hl7'




producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000,
'security.protocol' : 'SSL',
'ssl.key.location' : 'key2026.pem', #This is the private_key.txt from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
'ssl.certificate.location': 'cert2026.pem', #this is the  Certificate.txt and certificate_chain.txt files merged together - the certificate_chain goes on bottom. from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
'ssl.ca.location' : 'CARoot.pem', #This is the Amazon Root Public CARoots from here: https://www.amazontrust.com/repository/
'ssl.key.password' : 'abcsd', # This is the passphrase  from this process for the certificate that secures the MSK cluster : https://docs.aws.amazon.com/acm/latest/userguide/export-private.html 
    'delivery.report.only.error' : True
})



def lambda_handler(event, context):
    logging.info("Running this lambda with")
    logging.info(f"BROKER_URL: {KAFKA_BROKER}")
    logging.info(f"TOPIC: {KAFKA_TOPIC}")
    # logging.info(f"raw bucket: {s3_bucket}")
    logging.info(f"Lambda function ARN: {context.invoked_function_arn}")
    logging.info(f"CloudWatch log stream name:{context.log_stream_name}")
    logging.info(f"CloudWatch log group name:{context.log_group_name}")
    logging.info(f"Lambda Request ID:{context.aws_request_id}")
    logging.info(f"Lambda function memory limits in MB: {context.memory_limit_in_mb}")
    #get the lambda event data
    count = 1
    logging.info(event)

    for contents in event['tasks']:
        logging.info(f"Sednging Page number: {count}")
        logging.info(f"each content of the page is {contents}")
        s3_bucket = contents['s3Bucket']
        s3_key = contents['s3Key'] #here is the manifest really
        logging.info(f"{s3_key=}")
        obj_s3 = s3_resource.Object(bucket_name=s3_bucket, key=s3_key) # From here we are getting message content and sending to MSK - this needs to be it's own Lambda, so feed in the entire manifest to the lambda we create from here.
        logging.info(f"{obj_s3=}")
        response_s3 = obj_s3.get()
        logging.info(f"{response_s3=}")
        msg = response_s3['Body'].read().decode()
        msg = json.loads(msg)
        logging.info(f"{msg=}")
        msg["log"].append(
         {
            "function": context.function_name,
            "timestamp":datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "result": "OK"
        })
        msg = json.dumps(msg)
        logging.info(f"{msg=}")
        producer.produce(
            KAFKA_TOPIC,
            msg,
            callback=lambda err, original_msg=msg: delivery_report(err, original_msg),
        )
        #producer.flush()
        producer.poll(0)
        logging.info(f"sent msg with TOPIC {KAFKA_TOPIC} to BROKER {KAFKA_BROKER}")
        logging.info(f"{msg=}")

        count += 1


def delivery_report(err, msg):
    if err is not None:
        logging.info('Message delivery failed: {}'.format(err))
    else:
        return('null')
        #logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def send_msg_async(msg):
    #logging.info("Sending message")
    try:
        # msg_json_str = json.dumps({'data': json.loads(msg)})
        # logging.info(f"{msg_json_str=}")
        logging.info(f"{msg=}")
        producer.produce(
            KAFKA_TOPIC,
            msg,
            callback=lambda err, original_msg=msg: delivery_report(err, original_msg),
        )
        #producer.flush()
        producer.poll(0)
        logging.info(f"sent msg with TOPIC {KAFKA_TOPIC} to BROKER {KAFKA_BROKER}")
        pprint.plogging.info(msg)
    except Exception as ex:
        logging.info("Error : ", ex)


def serialize_bytes(obj):
    if isinstance(obj, bytes):
        return obj.decode()
    raise TypeError ("Type %s is not serializable" % type(obj))

