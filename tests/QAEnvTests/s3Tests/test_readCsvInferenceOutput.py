import csv
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_inferenceOutputBucket
from testBase.QAEnv.retrieveShortTermForecastKey import getObjKeyList


class Test_JsonFile:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_inference_output = get_qa_inferenceOutputBucket()

    prefix = 'Test/Test/2022/11/'

    objKeyLst = getObjKeyList().getObjKeyLstFromSpecificFolder(qa_inference_output, prefix)
    print(len(objKeyLst))

    def test_readCsvFile(self):

        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_client = self.s3_session.client('s3')

        for key in self.objKeyLst:
            obj = s3_client.get_object(Bucket=self.qa_inference_output, Key=key)
            # print(obj['Body'])

            lines = obj['Body'].read().decode('utf-8').splitlines(True)
            print('\n')
            # print(lines)
            reader = csv.DictReader(lines)

            for row in reader:
                unitId = row['unitId']
                census = int(row['census'])
                print(unitId, census)
                # print(type (census))



