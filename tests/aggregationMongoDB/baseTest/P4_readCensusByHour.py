from utilities.readConfig import *
from tests.aggregationMongoDB.baseTest.P4_objectKeysForTestData import *
from tests.aggregationMongoDB.baseTest.P4_readCensusByHour import *

def inferenceInputCsv(s3_session):

    getter = getObjectKeys(s3_session)
    # Get object keys for TestClient
    inferenceInputKeys = getter.getInferenceInputObjKeys_S3Test()
    # Get the keyObj for the latest file
    keyObj = inferenceInputKeys[-1]
    print(keyObj)
    if keyObj is None:
        raise Exception("keyObj is empty")

    session = s3_session
    s3_client = session.client('s3')

    response = s3_client.get_object(Bucket= get_qa_inferenceInputBucket(), Key= keyObj)
    csv_data = response['Body'].read().decode('utf-8')
    csv_lines = csv_data.split('\n')
    # get title from 3 columns


    return csv_lines