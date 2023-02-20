import json
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_outputPublisherBucket
from testBase.QAEnv.retrieveJsonObjKey import getObjKeyList
from urllib.parse import unquote


class Test_JsonFile_002:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_rawbucket_name = get_qa_outputPublisherBucket()

    prefix = 'Test/2023/1/19/'

    def test_readJsonFile(self):
        objKeyLst = getObjKeyList().getObjKeyLstFromSpecificFolder(self.qa_rawbucket_name, self.prefix)

        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_client = self.s3_session.client('s3')

        for key in objKeyLst:
            obj = s3_client.get_object(Bucket=self.qa_rawbucket_name, Key=unquote(key))
            jsonBody = json.loads(obj['Body'].read())
            print(key, '\n')
            print(jsonBody, '\n')







