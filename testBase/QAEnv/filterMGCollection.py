import pymongo
from utilities.readConfig import *
import certifi


class MGFilter:
    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000, tlsCAFile=ca)
    databaseName = get_qa_mongo_database()
    parsedhl7Collection = get_qa_mongo_parsedhl7_collection()
    unitTaxonomy = get_qa_mongo_unitTaxonomy_collection()
    screensQuality = get_qa_mongo_qualityScreens_collection()
    extractedAdt = get_qa_mongo_extractedADT_collection()

    def parsedHL7_positiveData(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        query = {"eventId": {"$gte": "testqa-T8-mongo-Positive-MT", "$lte": "testqa-T8-mongo-Positive-MTz"}}

        docs = parsedhl7.find(query)
        return docs

    def parsedHL7_testClient(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        query = {"clientId": {"$eq": "TestClient"}}

        docs = parsedhl7.find(query).sort([('timestamp', -1)])
        return docs

    def unitTaxonomyFilterTestClient(self):
        dbname = self.client.get_database(self.databaseName)
        unitTax = dbname.get_collection(self.unitTaxonomy)
        query = {"client.clientId": {"$eq": "TestClient"}}

        docs = unitTax.find(query)
        return docs

    def qualityScreensFilter(self):
        dbname = self.client.get_database(self.databaseName)
        unitTax = dbname.get_collection(self.screensQuality)

        docs = unitTax.find()
        return docs

    def extractedAdtEventsFilter_TestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "TestClient"}}
        docs = extractedAdt.find(query).sort([('timestamp', -1)])
        return docs

    def parsedHL7_ESTAGGR(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)

        query = {"clientId": {"$eq": "ESTAGRTestClient"}}
        docs = parsedhl7.find(query).sort([('timestamp', -1)])
        return docs

    def extractedAdt_ESTAGGR_false_WIP(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "ESTAGRTestClient"}, "isDocWIP": {"$eq": False}}
        docs = extractedAdt.find(query).sort([('timestamp', -1)])
        return docs

    def extractedAdt_ESTAGGR(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.extractedAdt)

        query = {"clientId": {"$eq": "ESTAGRTestClient"}}
        docs = parsedhl7.find(query).sort([('timestamp', -1)])
        return docs




