from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_rawBucket


class Test_MultiTenancy_003:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_rawbucket_name = get_qa_rawBucket()
    # prefix = 'Test/2022/'
    prefix = 'Test/2022/11/1/'
    keyObjLst = []

    def test_countObjInSpecificFolder(self):
        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_resources_object = self.s3_session.resource('s3')
        my_bucket = s3_resources_object.Bucket(self.qa_rawbucket_name)

        for object_summary in my_bucket.objects.filter(Prefix=self.prefix):
            print(object_summary.key)
            self.keyObjLst.append(object_summary.key)
        print('Total Objects in Test/2022/11/1 : ', len(self.keyObjLst))