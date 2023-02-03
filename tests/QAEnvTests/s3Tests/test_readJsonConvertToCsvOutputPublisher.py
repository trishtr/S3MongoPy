import pandas as pd
import json
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_outputPublisherBucket
from testBase.QAEnv.retrieveJsonObjKey import getObjKeyList


class Test_JsonFile_002:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_rawbucket_name = get_qa_outputPublisherBucket()

    prefix = 'Test/2023/1/19/c62e1202'

    objKeyLst = getObjKeyList().getObjKeyLstFromSpecificFolder(qa_rawbucket_name, prefix)

    # print(len(objKeyLst))

    def test_readJsonFile(self):
        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_client = self.s3_session.client('s3')

        for key in self.objKeyLst:
            obj = s3_client.get_object(Bucket=self.qa_rawbucket_name, Key=key)

            # json.load() convert json string to dict
            jsonBody = json.loads(obj['Body'].read())
            # print (type (obj['Body']))
            print(jsonBody['payload'])
            print(type(jsonBody['payload']))

            census_data = jsonBody['payload'][0].get('census_data')
            print(type(jsonBody['payload'][0].get('census_data')))

            json_object = json.dumps(census_data, indent=3)

            # # Writing to sample.json
            with open("outputPublisher.json", "w") as outfile:
                outfile.write(json_object)

            # convert to csv file
            with open('outputPublisher.json', encoding='utf-8') as inputfile:
                df = pd.read_json(inputfile)

                df.to_csv('outputPublisher.csv', encoding='utf-8', index=False)




