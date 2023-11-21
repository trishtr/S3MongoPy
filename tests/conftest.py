import pytest
import certifi
import pymongo
import json
from bson import json_util
from utilities.readConfig import *
import boto3
import os



# Connect to S3
@pytest.fixture(scope= "session")
def s3_session():
    stage = os.environ.get('STAGE',None)
    if stage == 'qa':
        session  = boto3.Session()
        return session

    else:
        session = boto3.Session(
            aws_access_key_id=get_qa_accessKey(),
            aws_secret_access_key=get_qa_secretKey(),
            aws_session_token = get_qa_sessionToken(),
            region_name= get_qa_regionName()
        )
    return session



# Connect to MongoDB
@pytest.fixture(scope = "session", autouse = True)
def MGConnect():
    try:
        ca = certifi.where()
        client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000,tlsCAFile=ca)
        databaseName = get_qa_mongo_database()
        dbname = client.get_database(databaseName)
        print("Create connecting session")

        yield dbname

    except ConnectionError as e:
        print(f"Connection error: {e}")
        raise ConnectionError("Failed to establish a MongoDB connection")


    print("Close the connecting session")
    client.close()

@pytest.fixture(scope = "session", autouse = True)
def getDatabaseName(MGConnect):
    dbname = MGConnect
    return dbname



# # Part 1: Aggregation set up
# # insert TenantConfig and delete after finishing up the test




@pytest.fixture(scope = "session", autouse = True)
def P1_1_EST_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)

    query = {"clientId": "ESTAGRTestClient"}
    tenant.delete_many(query)
    print("P1_EST_tenantConfiguration old dataset is deleted")

    file = open("./tests/aggregationMongoDB/dataset/P1_ESTtenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P1_EST_tenantConfiguration is successfully inserted")

    yield





# # # # # # EST insert parsedhl7 documents and delete after testing
@pytest.fixture(scope = "session", autouse = True)
def P1_2_EST_parsedHL7(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "ESTAGRTestClient"}
    hl7.delete_many(query)
    print("P1_EST_HL7 old docs are deleted")

    extractedAdt = dbname.get_collection("extractedAdtEvents")
    extractedAdt.delete_many(query)
    print("P1_EST_adt old docs are deleted")

    pe = dbname.get_collection("patientEncounters")
    pe.delete_many(query)
    print("P1_EST_PE old docs are deleted")

    et = dbname.get_collection("encounterTrackings")
    et.delete_many(query)
    print("P1_EST_ET old docs are deleted")

    file = open("./tests/aggregationMongoDB/dataset/P1_EST_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P1_EST_HL7 documents are successfully inserted")


# # # # # # EST filter and return the parsedhl7_ test data
@pytest.fixture(scope = "function")
def P1_EST_HL7_filter(getDatabaseName):
    dbname = getDatabaseName
    parsedHL7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "ESTAGRTestClient"}
    docs = parsedHL7.find(query)

    return docs


# # # # # # filter adt EST data
@pytest.fixture(scope = "function")
def P1_EST_extractedAdt_filter(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    query = {"clientId": "ESTAGRTestClient"}
    docs = extractedAdt.find(query)

    return docs


# Part 1 : NZT
# insert TenantConfig and delete after finishing up the test

@pytest.fixture(scope = "session", autouse = True)
def P1_1_NZT_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)

    query = {"clientId": "NZTAGRTestClient"}
    tenant.delete_many(query)
    print("P1_NZT_tenantConfiguration is deleted")

    file = open("./tests/aggregationMongoDB/dataset/P1_NZTtenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P1_NZT_tenantConfiguration is successfully inserted")

    yield



# NZT insert parsedhl7 documents and delete after testing
@pytest.fixture(scope = "session", autouse = True)
def P1_2_NZT_parsedHL7(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "NZTAGRTestClient"}

    hl7.delete_many(query)
    print("P1_NZT_HL7 old docs are deleted")

    extractedAdt = dbname.get_collection("extractedAdtEvents")
    extractedAdt.delete_many(query)
    print("P1_NZT_adt old docs are deleted")

    pe = dbname.get_collection("patientEncounters")
    pe.delete_many(query)
    print("P1_NZT_PE old docs are deleted")

    et = dbname.get_collection("encounterTrackings")
    et.delete_many(query)
    print("P1_NZT_ET old docs are deleted")

    file = open("./tests/aggregationMongoDB/dataset/P1_NZT_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P1_NZT_HL7 documents are successfully inserted")


# NZT filter and return the parsedhl7_ test data
@pytest.fixture(scope = "function")
def P1_NZT_HL7_filter(getDatabaseName):
    dbname = getDatabaseName
    parsedHL7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "NZTAGRTestClient"}
    docs = parsedHL7.find(query)

    return docs



# NZT extractedAdt Events
@pytest.fixture(scope = "function")
def P1_NZT_extractedAdt_filter(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    query = {"clientId": "NZTAGRTestClient"}
    docs = extractedAdt.find(query)

    return docs





# Part2 Aggregation set up :
# PETestClient_TenantConfig insert
@pytest.fixture(scope = "session", autouse = True)
def P2_PETestClient_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)
    query = {"clientId": "PETestClient"}
    tenant.delete_many(query)
    print("P2_PETestClient_tenantConfiguration old dataset is deleted")


    file = open("./tests/aggregationMongoDB/dataset/P2_PEtenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P2_PETestClient_tenantConfiguration is successfully inserted")

    yield



# PETestClient inserted into unit taxonomy
@pytest.fixture(scope = "session", autouse = True)
def P2_PETestClient_UnitTaxonomy(getDatabaseName):
    dbname = getDatabaseName
    unitTaxonomy = "unitTaxonomy"
    tenant = dbname.get_collection(unitTaxonomy)

    query = {"client.clientId": "PETestClient"}
    tenant.delete_many(query)
    print("P2_PETestClient_unitTaxonomy old dataset is deleted")

    file = open("./tests/aggregationMongoDB/dataset/P2_PETestClient_unitTaxonomy.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P2_PETestClient_unitTaxonomy new dataset is successfully inserted")




# PETestClient inserted into parsed-hl7
@pytest.fixture(scope = "session", autouse = True)
def P2_PETestClient_setup(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "PETestClient"}

    hl7.delete_many(query)
    print("P2_PETestClient old parsed documents are successfully deleted")

    extractedAdt = dbname.get_collection("extractedAdtEvents")
    extractedAdt.delete_many(query)
    print("P2_PETestClient old extractedAdtEvents docs are deleted")

    pe = dbname.get_collection("patientEncounters")
    pe.delete_many(query)
    print("P2_PETestClient old PatientEncounter docs are deleted")

    et = dbname.get_collection("encounterTrackings")
    et.delete_many(query)
    print("P2_PETestClient old EncounterTrackings docs are deleted")

    file = open("./tests/aggregationMongoDB/dataset/P2_PETestClient_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P2_PETestClient parsed documents are successfully inserted")


# # PETestClient filter in tenantConfiguration
@pytest.fixture(scope = "function")
def P2_PETestClient_tenantConfiguration_docs(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = dbname.get_collection("tenantConfiguration")
    query = {"clientId": "PETestClient"}
    docs = tenantConfig.find(query)
    return docs


# # # # PETestClient filter in extractedAdt with qualityScreens.summary = fail
@pytest.fixture(scope = "function")
def P2_PETestClient_extractedAdt_failQualityScreens_docs(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    count=  {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "fail"}}
    doc_count = extractedAdt.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data fail qualityScreens are found in extractedAdtEvents collection")

    filter = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "fail"}}
    docs = extractedAdt.find(filter)

    return docs



# PETestClient filter in extractedAdt with qualityScreens.summary = pass
@pytest.fixture(scope = "function")
def P2_PETestClient_extractedAdt_passQualityScreens_docs(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    count=  {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"}}
    doc_count = extractedAdt.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data pass qualityScreens are found in extractedAdtEvents collection")

    filter = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"}}
    docs = extractedAdt.find(filter)

    return docs


# # PETestClient_extractedAdt_passQualityScreens_includeInCensus_True
@pytest.fixture(scope = "function")
def P2_PETestClient_extractedAdt_passQualityScreens_includeInCensus_True_docs(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    count=  {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"}}
    doc_count = extractedAdt.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data pass qualityScreens are found in extractedAdtEvents collection")

    filter = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"},"includeInCensus": True}
    docs = extractedAdt.find(filter)

    return docs


# # PETestClient, includeInCensus : false
@pytest.fixture(scope = "function")
def P2_PETestClient_extractedAdt_passQualityScreens_includeInCensus_False_docs(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    count=  {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"}}
    doc_count = extractedAdt.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data pass qualityScreens are found in extractedAdtEvents collection")

    filter = {"clientId": {"$eq": "PETestClient"}, "qualityScreens.summary" : {"$eq": "pass"},"includeInCensus": False}
    docs = extractedAdt.find(filter)

    return docs

# # # # # PETestClient filter mocking data in patientEncounters
@pytest.fixture(scope = "function")
def P2_PETestClient_PatientEncounters_docs(getDatabaseName):
    dbname = getDatabaseName
    patientEncounters = dbname.get_collection("patientEncounters")
    count = {"clientId": "PETestClient"}
    doc_count = patientEncounters.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in patientEncounters collection")

    filter = {"clientId": "PETestClient"}
    docs = patientEncounters.find(filter)

    return docs

# P2_ UnitTaxonomy docs:
@pytest.fixture(scope = "function")
def P2_PETestClient_unitTaxonomy_docs(getDatabaseName):
    dbname = getDatabaseName
    unitTaxonomy = dbname.get_collection("unitTaxonomy")
    count = {"client.clientId": "PETestClient"}
    doc_count = unitTaxonomy.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in unitTaxonomy collection")

    filter = {"client.clientId": "PETestClient"}
    docs = unitTaxonomy.find(filter)

    return docs




# Part 3,4 Aggregation set up:
# ETTestClient tenantConfiguration insert:
@pytest.fixture(scope = "session", autouse = True)
def P3_4_ETTestClient_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)
    query = {"clientId": "ETTestClient"}
    tenant.delete_many(query)

    print("P3_ETTestClient_tenantConfiguration is deleted ~ cleaning the old dataset")

    file = open("./tests/aggregationMongoDB/dataset/P3_ETTestClient_tenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P3_ETTestClient_tenantConfiguration is successfully inserted")





# ETTestClient inserted into unit taxonomy
@pytest.fixture(scope = "session", autouse = True)
def P3_ETTestClient_UnitTaxonomy(getDatabaseName):
    dbname = getDatabaseName
    unitTaxonomy = "unitTaxonomy"
    ut = dbname.get_collection(unitTaxonomy)

    query = {"client.clientId": "ETTestClient"}
    ut.delete_many(query)
    print("P3_ETTestClient_unitTaxonomy is deleted ~ cleaning up old data")

    file = open("./tests/aggregationMongoDB/dataset/P3_ETTestClient_unitTaxonomy.json")
    sampleData = json.load(file,object_hook=json_util.object_hook)
    for data in sampleData:
        insertedDoc = ut.insert_one(data)
    print("P3_ETTestClient_unitTaxonomy is successfully inserted")



# ETTestClient inserted into parsed-hl7
@pytest.fixture(scope = "session", autouse = True)
def P3_ETTestClient_parsedHL7(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "ETTestClient"}

    hl7.delete_many(query)
    print("P3_ETTestClient_parsed HL7 are deleted ~ cleaning up old dataset")

    extractedAdt = dbname.get_collection("extractedAdtEvents")
    extractedAdt.delete_many(query)
    print("P3_ETTestClient_adt old docs are deleted")

    pe = dbname.get_collection("patientEncounters")
    pe.delete_many(query)
    print("P3_ETTestClient old PE docs are deleted")

    et = dbname.get_collection("encounterTrackings")
    et.delete_many(query)
    print("P3_ETTestClient old ET docs are deleted")

    file = open("./tests/aggregationMongoDB/dataset/P3_ETTestClient_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P3_ETTestClient documents are successfully inserted")



# # P3_4: patientEncounters hasCensusEffect : true, qualityscreen: pass
@pytest.fixture(scope = "function")
def P3_ETTestClient_PatientEncounters_docs(getDatabaseName):
    dbname = getDatabaseName
    patientEncounters = dbname.get_collection("patientEncounters")
    count = {"clientId": "ETTestClient"}
    doc_count = patientEncounters.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in patientEncounters collection")

    filter = {"clientId": "ETTestClient", "hasCensusEffect": True, "events.includeInCensus": True, "qualityScreens.summary": "pass"}
    docs = patientEncounters.find(filter)

    return docs


# # P4: EncounterTracking clientId: ETTestClient, trackingStatus.includeInCensus: True
@pytest.fixture(scope = "function")
def P4_ETTestClient_EncounterTracking_docs(getDatabaseName):
    dbname = getDatabaseName
    encounterTracking = dbname.get_collection("encounterTracking")
    count = {"clientId": "ETTestClient"}
    doc_count = encounterTracking.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in encounterTracking collection")

    filter = {"clientId": "ETTestClient", "trackingStatus.includeInCensus": True}
    docs = encounterTracking.find(filter)

    return docs

# # P4: EncounterTracking clientId : "ETTestClient"
@pytest.fixture(scope = "function")
def P4_ETTestClient_EncounterTracking_filterByClientId_docs(getDatabaseName):
    dbname = getDatabaseName
    encounterTracking = dbname.get_collection("encounterTracking")
    count = {"clientId": "ETTestClient"}
    doc_count = encounterTracking.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in encounterTracking collection")

    filter = {"clientId": "ETTestClient"}
    docs = encounterTracking.find(filter)

    return docs

# # ETTestClient unitTaxonomy docs
@pytest.fixture(scope = "function")
def P3_ETTestClient_unitTaxonomy_docs(getDatabaseName):
    dbname = getDatabaseName
    unitTaxonomy = dbname.get_collection("unitTaxonomy")
    count = {"client.clientId": "ETTestClient"}
    doc_count = unitTaxonomy.count_documents(count)
    if doc_count == 0:
        raise Exception("No test data are found in unitTaxonomy collection")

    filter = {"client.clientId": "ETTestClient"}
    docs = unitTaxonomy.find(filter)

    return docs

# # P4: encounterTrackings has includeInCensus : true, CAYUGA
# # For investigation only
# # @pytest.fixture(scope = "function")
# # def P4_CAYUGA_EncounterTracking_docs(getDatabaseName):
# #     dbname = getDatabaseName
# #     encounterTracking = dbname.get_collection("encounterTracking")
# #     count = {"clientId": "CAYUGA"}
# #     doc_count = encounterTracking.count_documents(count)
# #     if doc_count == 0:
# #         raise Exception("No test data are found in encounterTracking collection")

# #     filter = {"clientId": "CAYUGA", "trackingStatus.includeInCensus": True, "trackingStatus.localDateTime": { "$gte": "2021-02-22 10:00:00" , "$lte" :"2021-02-24 10:00:00"}}
# #     docs = encounterTracking.find(filter)

# #     return docs


# # P4: encounterTrackings

# # # Part 4: set up
# # # ETTestClient exportJobs ~ to export census Number from mongo to inference input
@pytest.fixture(scope = "session", autouse = True)
def P4_ETTestClient_exportJobs(getDatabaseName):
    dbname = getDatabaseName
    export = dbname.get_collection("exportJobs")
    query = {"args.clientId": "ETTestClient"}

    export.delete_many(query)
    print("P4_ETTestClient_exportJobs are deleted ~ cleaning up old dataset")

    file = open("./tests/aggregationMongoDB/dataset/P4_exportJobs.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = export.insert_one(data)
    print("P4_exportJobs_ETTestClient new documents are successfully inserted")







# Set up for inference ML testing- S3
# Mongo Tenant Configuration - S3 inference ML testing

