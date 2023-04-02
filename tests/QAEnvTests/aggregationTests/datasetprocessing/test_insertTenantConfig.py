import pymongo
from utilities.readConfig import *
import certifi
import json


class Test_insertConfigDoc:
    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000, tlsCAFile=ca)
    databaseName = get_qa_mongo_database()
    parsedhl7Collection = get_qa_mongo_parsedhl7_collection()
    unitTaxonomy = get_qa_mongo_unitTaxonomy_collection()
    screensQuality = get_qa_mongo_qualityScreens_collection()
    extractedAdt = get_qa_mongo_extractedADT_collection()
    tenantConfig = "tenantConfiguration"

    def test_insertConfigDoc(self):
        dbname = self.client.get_database(self.databaseName)
        tenant = dbname.get_collection(self.tenantConfig)
        file = open("./tests/QAEnvTests/aggregationTests/tenantconfig.json")
        sampleData = json.load(file)
        for data in sampleData:
            insertedDoc = tenant.insert_one(data)
            print(insertedDoc.inserted_id)
