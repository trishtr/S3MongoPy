import pytest
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_rawBucket


class Test_S3_Connection:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_rawbucket_name = get_qa_rawBucket()
    qa_bucket_list = []

    def test_s3bucket_exist(self):
        # call s3_connect class
        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        s3_resources_object = self.s3_session.resource('s3')

        for bucket in s3_resources_object.buckets.all():
            self.qa_bucket_list.append(bucket.name)

        print('List of QA S3 buckets : ')
        print(self.qa_bucket_list)
        print('QA raw bucket name : ' + self.qa_rawbucket_name)
        assert self.qa_rawbucket_name in self.qa_bucket_list




