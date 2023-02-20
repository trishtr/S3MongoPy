from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken


class getObjKeyList:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()

    # prefix = 'TestClient/2022/'

    def getObjKeyLstFromSpecificFolder(self, bucketName, prefix):
        objectKeyLst = []

        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_resources_object = self.s3_session.resource('s3')
        my_bucket = s3_resources_object.Bucket(bucketName)

        for object_summary in my_bucket.objects.filter(Prefix=prefix):
            # print(object_summary.key)
            key = object_summary.key
            if key.endswith("ShortTermForecast.csv"):
                print(key)
                objectKeyLst.append(object_summary.key)

        return objectKeyLst
