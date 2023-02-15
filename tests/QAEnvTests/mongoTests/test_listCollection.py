import pymongo

from utilities.readConfig import *
import certifi


class Test_MongoConnection:

    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000,tlsCAFile=ca)

    def test_printCollection(self):
        data_base = self.client.get_database(get_qa_mongo_database())
        collection_name_lst = data_base.list_collection_names()
        for collection in collection_name_lst:
            print(collection)


