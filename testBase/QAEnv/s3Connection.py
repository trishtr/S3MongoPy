import boto3
from boto3.session import Session


class s3Connect:

    def __init__(self, access_key, secret_key, session_token, region_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.region_name = region_name

    def create_s3_session(self):
        session = boto3.Session(aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key,
                                aws_session_token=self.session_token,
                                region_name=self.region_name)

        return session

