from utilities.readConfig import *

def test_P4_039_inferenceInput_folderStructure(s3_session):
    s3 = s3_session
    client = s3.client('s3')

    paginator = client.get_paginator('list_objects')
    for page in paginator.paginate(Bucket = get_qa_inferenceInputBucket()):
        for obj in page.get('Contents', []):
            object_key = obj['Key']
            if get_testClientId_1() in object_key:
                keyString = object_key.split("/")
                assert keyString[0] == get_testClientId_1()
                assert keyString[1] == get_testLocationId_1()
                assert int(keyString[2]) >= 2022
                assert 1 <= int(keyString[3]) <= 12
                assert 1<= int(keyString[4]) <= 31
                print(object_key)