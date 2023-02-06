import pymongo

from utilities.readConfig import get_qa_mongo_database, get_qa_mongo_parsedhl7_collection, \
    get_qa_mongo_connectionString, get_qa_mongo_unitTaxonomy_collection
import certifi


class MGFilter:
    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000, tlsCAFile=ca)
    databaseName = get_qa_mongo_database()
    parsedhl7Collection = get_qa_mongo_parsedhl7_collection()
    unitTaxonomy = get_qa_mongo_unitTaxonomy_collection()

    def parsedHL7_positiveData(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        query = {"eventId": {"$gte": "testqa-T8-mongo-Positive-MT", "$lte": "testqa-T8-mongo-Positive-MTz"}}

        docs = parsedhl7.find(query)
        return docs

    def parsedHL7_latest(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)

        docs = parsedhl7.find().sort([('timestamp', -1)]).limit(100)
        return docs

    def unitTaxonomyFilterTestClient(self):
        dbname = self.client.get_database(self.databaseName)
        unitTax = dbname.get_collection(self.unitTaxonomy)
        query = {"client.clientId": {"$eq": "TestClient"}}

        docs = unitTax.find(query)
        return docs







