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
    patientEncounters = 'patientEncounters'
    encounterTrackings = 'encounterTracking'

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
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_ESTAGGR(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)

        query = {"clientId": {"$eq": "ESTAGRTestClient"}}
        docs = extractedAdt.find(query)
        return docs

    def parsedHL7_ESTNEGAGR(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        query = {"$or": [{"clientId": {"$regex": "^ESTNEGAGRTestClient"}}
            , {"eventId": {"$regex": "^EST-testqa-arg-neg"}}]}

        docs = parsedhl7.find(query).sort([('timestamp', -1)])
        return docs

    def extractedAdt_ESTNEGAGR(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)

        query = {"$or": [{"clientId": {"$regex": "^ESTNEGAGRTestClient"}}
            , {"eventId": {"$regex": "^EST-testqa-arg-neg"}}]}
        docs = extractedAdt.find(query)
        return docs

    def parsedHL7_NZTAGR(self):
        dbname = self.client.get_database(self.databaseName)
        parsedhl7 = dbname.get_collection(self.parsedhl7Collection)
        query = {"clientId": "NZTAGRTestClient"}
        docs = parsedhl7.find(query)
        return docs

    def extractedAdt_NZTAGR(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)

        query = {"clientId": {"$eq": "NZTAGRTestClient"}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_NZTAGR_false_WIP(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "NZTAGRTestClient"}, "isDocWIP": {"$eq": False}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_qualityScreens_fail_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary": {"$eq": "fail"}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_qualityScreens_pass_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary": {"$eq": "pass"}}
        docs = extractedAdt.find(query)
        return docs

    def patientEncounters_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        patientEn = dbname.get_collection(self.patientEncounters)
        query = {"clientId": {"$eq": "PETestClient"}}
        docs = patientEn.find(query)
        return docs

    def extractedAdt_screenquality_pass_unitId_exists_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary": {"$eq": "pass"},
                 "unitId": {"$exists": True}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_screenquality_pass_unitType_exists_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary": {"$eq": "pass"},
                 "unitType": {"$exists": True}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_screenquality_pass_dischargeDateTime_exists_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary": {"$eq": "pass"},
                 "dischargeDateTime": {"$exists": True}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_screenquality_pass_excludeCensus_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"},
                 "qualityScreens.summary": {"$eq": "pass"},
                 "patientClass": {"$eq": "O"},
                 "patientType": {"$in": ["OPD", "SERIES", "SDS", "PAT"]}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_screenquality_pass_includeCensus_patientClass_O_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"},
                 "qualityScreens.summary": {"$eq": "pass"},
                 "patientClass": {"$eq": "O"},
                 "patientType": {"$nin": ["OPD", "SERIES", "SDS", "PAT"]}}
        docs = extractedAdt.find(query)
        return docs

    def extractedAdt_screenquality_pass_includeCensus_patientClass_I_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "PETestClient"},
                 "qualityScreens.summary": {"$eq": "pass"},
                 "patientClass": {"$eq": "I"},
                 "patientType": {"$in": ["OPD", "SERIES", "SDS", "PAT"]}}
        docs = extractedAdt.find(query)
        return docs

    def unitTaxonomy_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        unitTax = dbname.get_collection(self.unitTaxonomy)
        query = {"client.clientId": {"$eq": "PETestClient"}}

        docs = unitTax.find(query)
        return docs

    def extractedAdt_screenquality_pass_TestClient(self):
        dbname = self.client.get_database(self.databaseName)
        extractedAdt = dbname.get_collection(self.extractedAdt)
        query = {"clientId": {"$eq": "TestClient"}, "qualityScreens.summary": {"$eq": "pass"}}
        docs = extractedAdt.find(query)
        return docs

    def patientEncounters_TestClient(self):
        dbname = self.client.get_database(self.databaseName)
        patientEn = dbname.get_collection(self.patientEncounters)
        query = {"clientId": {"$eq": "TestClient"}}
        docs = patientEn.find(query)
        return docs

    def patientEncounters_hasCensusEffect_true_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        patientEn = dbname.get_collection(self.patientEncounters)
        query = {"clientId": {"$eq": "PETestClient"}, "hasCensusEffect": True, "visitNumber": "PEFARGVN011"}
        docs = patientEn.find(query)
        return docs

    def encounterTracking_PETestClient(self):
        dbname = self.client.get_database(self.databaseName)
        encounterTrackings = dbname.get_collection(self.encounterTrackings)
        query = {"clientId": {"$eq": "PETestClient"}, "visitNumber": "PEFARGVN011"}
        docs = encounterTrackings.find(query).sort([('dateUTC', 1)])
        return docs






