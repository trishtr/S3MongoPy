from configparser import ConfigParser

config = ConfigParser()
file = "./utilities/config.ini"
config.read(file)

def get_qa_accessKey():
    qa_access_key = config.get('qa_environment', 'access_key')
    return qa_access_key

def get_qa_secretKey():
    qa_secret_key = config.get('qa_environment', 'secret_key')
    return qa_secret_key

def get_qa_sessionToken():
    qa_session_token = config.get('qa_environment', 'session_token')
    return qa_session_token

def get_qa_regionName():
    qa_region_name = config.get('qa_environment', 'region_name')
    return qa_region_name

def get_qa_rawBucket():
    qa_raw_bucket = config.get('qa_environment', 'raw_bucket')
    return qa_raw_bucket

def get_qa_outputPublisherBucket():
    qa_output_publisher_bucket = config.get('qa_environment', 'output_publisher')
    return qa_output_publisher_bucket

def get_qa_inferenceOutputBucket():
    qa_inference_output_bucket = config.get('qa_environment', 'inference_output')
    return qa_inference_output_bucket

