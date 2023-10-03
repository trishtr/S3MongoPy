import pytest
import certifi
import pymongo
import os
import json
from utilities.readConfig import *

@pytest.fixture(scope = "session", autouse = True)
def MGConnect():
    ca = certifi.where()
    client = pymongo.MongoClient(get_qa_mongo_connectionString(), serverSelectionTimeoutMS=5000,tlsCAFile=ca)
    databaseName = get_qa_mongo_database()
    dbname = client.get_database(databaseName)
    print("Create connecting session")

    yield dbname

    print("Close the connecting session")
    client.close()

@pytest.fixture(scope = "session", autouse = True)
def getDatabaseName(MGConnect):
    dbname = MGConnect
    return dbname

# Part 1: Aggregation set up
# insert TenantConfig and delete after finishing up the test
@pytest.fixture(scope = "session", autouse = True)
def P1_1_EST_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)
    file = open("./tests/aggregationMongoDB/dataset/P1_ESTtenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P1_EST_tenantConfiguration is successfully inserted")

    yield

    query = {"clientId": "ESTAGRTestClient"}
    tenant.delete_many(query)
    print("P1_EST_tenantConfiguration is deleted")

# EST insert parsedhl7 documents and delete after testing
@pytest.fixture(scope = "session", autouse = True)
def P1_2_EST_parsedHL7(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    file = open("./tests/aggregationMongoDB/dataset/P1_EST_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P1_EST_HL7 documents are successfully inserted")


# EST filter and return the parsedhl7_ test data
@pytest.fixture(scope = "session")
def P1_EST_HL7_filter(getDatabaseName):
    dbname = getDatabaseName
    parsedHL7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "ESTAGRTestClient"}
    docs = parsedHL7.find(query)

    yield docs

    parsedHL7.delete_many(query)
    print("P1_EST_HL7 docs are deleted")



# EST extractedAdt Events
@pytest.fixture(scope = "module")
def P1_EST_extractedAdt_filter(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    query = {"clientId": "ESTAGRTestClient"}
    docs = extractedAdt.find(query)
    # return docs
    yield docs

    extractedAdt.delete_many(query)
    print("P1_EST_extractedAdt docs are deleted")



# Part 1 :
# insert TenantConfig and delete after finishing up the test
@pytest.fixture(scope = "session", autouse = True)
def P1_1_NZT_TenantConfiguration(getDatabaseName):
    dbname = getDatabaseName
    tenantConfig = "tenantConfiguration"
    tenant = dbname.get_collection(tenantConfig)
    file = open("./tests/aggregationMongoDB/dataset/P1_NZTtenantconfig.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = tenant.insert_one(data)
    print("P1_NZT_tenantConfiguration is successfully inserted")

    yield

    query = {"clientId": "NZTAGRTestClient"}
    tenant.delete_many(query)
    print("P1_NZT_tenantConfiguration is deleted")

# NZT insert parsedhl7 documents and delete after testing
@pytest.fixture(scope = "session", autouse = True)
def P1_2_NZT_parsedHL7(getDatabaseName):
    dbname = getDatabaseName
    hl7 = dbname.get_collection("parsed-hl7")
    file = open("./tests/aggregationMongoDB/dataset/P1_NZT_parsedHL7.json")
    sampleData = json.load(file)
    for data in sampleData:
        insertedDoc = hl7.insert_one(data)
    print("P1_NZT_HL7 documents are successfully inserted")


# NZT filter and return the parsedhl7_ test data
@pytest.fixture(scope = "session")
def P1_NZT_HL7_filter(getDatabaseName):
    dbname = getDatabaseName
    parsedHL7 = dbname.get_collection("parsed-hl7")
    query = {"clientId": "NZTAGRTestClient"}
    docs = parsedHL7.find(query)

    yield docs

    parsedHL7.delete_many(query)
    print("P1_NZT_HL7 docs are deleted")



# NZT extractedAdt Events
@pytest.fixture(scope = "session")
def P1_NZT_extractedAdt_filter(getDatabaseName):
    dbname = getDatabaseName
    extractedAdt = dbname.get_collection("extractedAdtEvents")
    query = {"clientId": "NZTAGRTestClient"}
    docs = extractedAdt.find(query)
    # return docs
    yield docs

    extractedAdt.delete_many(query)
    print("P1_NZT_extractedAdt docs are deleted")




