import pymongo
from utilities.readConfig import *
import certifi
import json


class Test_insertDoc:
    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000, tlsCAFile=ca)
    databaseName = get_qa_mongo_database()
    parsedhl7Collection = get_qa_mongo_parsedhl7_collection()
    unitTaxonomy = get_qa_mongo_unitTaxonomy_collection()
    screensQuality = get_qa_mongo_qualityScreens_collection()
    extractedAdt = get_qa_mongo_extractedADT_collection()
    tenantConfig = "tenantConfiguration"

    def test_insertDoc(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        file = open("./tests/QAEnvTests/aggregationTests/data.json")
        sampleData = json.load(file)
        for data in sampleData:
            insertedDoc = parsedhl7.insert_one(data)
            print(insertedDoc.inserted_id)
