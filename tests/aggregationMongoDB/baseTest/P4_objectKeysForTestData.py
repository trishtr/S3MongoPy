import boto3
from utilities.readConfig import *

class getObjectKeys():
    def __init__(self,s3_session):
        self.s3 = s3_session.client('s3')
        self.inferenceInputObjKeys = []
        self.inferenceInputObjKeys_S3test = []
        self.inferenceOutputObjKeys = []
        self.ETTestClientPrefix = "ETTestClient"
        self.TestClientPrefix = get_testClientId_1()


    def getInferenceInputObjKeys(self):

        paginator = self.s3.get_paginator('list_objects')
        for page in paginator.paginate(Bucket = get_qa_inferenceInputBucket()):
            for obj in page.get('Contents', []):
                object_key = obj['Key']
                if self.ETTestClientPrefix in object_key:
                    self.inferenceInputObjKeys.append(object_key)
        return self.inferenceInputObjKeys

    def getInferenceInputObjKeys_S3Test(self):

        paginator = self.s3.get_paginator('list_objects')
        for page in paginator.paginate(Bucket = get_qa_inferenceInputBucket()):
            for obj in page.get('Contents', []):
                object_key = obj['Key']
                if self.TestClientPrefix in object_key:
                    self.inferenceInputObjKeys_S3test.append(object_key)
        return self.inferenceInputObjKeys_S3test


    def getInferenceOutputObjKeys(self):
        objects = self.s3.list_objects(Bucket = get_qa_inferenceOutputBucket())

        for object in objects.get('Contents', []):
            key = object.get("Key")
            if key not in  self.inferenceOutputObjKeys:
                self.inferenceOutputObjKeys.append(key)
        return  self.inferenceOutputObjKeys