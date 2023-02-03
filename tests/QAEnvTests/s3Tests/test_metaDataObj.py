import pytest
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import get_qa_accessKey, get_qa_secretKey, get_qa_regionName, get_qa_sessionToken
from utilities.readConfig import get_qa_rawBucket


class Test_MultiTenancy_001:
    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_rawbucket_name = get_qa_rawBucket()
    prefix = 'Test/2022/11/1/'

    def test_object_exists_in_hl7bucket(self):

        # call s3_connect class
        self.connector = s3Connect(self.qa_access_key, self.qa_secret_key, self.qa_session_token, self.qa_region_name)

        self.s3_session = self.connector.create_s3_session()

        client = self.s3_session.client('s3')

        paginator = client.get_paginator('list_objects')

        operation_parameters = {'Bucket': self.qa_rawbucket_name,
                                'Prefix': self.prefix}

        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            # type of page is list, using loop to loop through the list of dic
            # print(page['Contents'])
            for content in page['Contents']:
                # print('Key : ' , content.get('Key'))
                # print('Size : ', content.get('Size'))
                print('Date Modified : ', content.get('LastModified'))
                # print('ETag : ' , content.get('ETag'))
                # print(content.get('Owner').get('DisplayName'))



















