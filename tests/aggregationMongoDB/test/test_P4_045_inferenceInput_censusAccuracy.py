from utilities.readConfig import *
from tests.aggregationMongoDB.baseTest.P4_objectKeysForTestData import *
from utilities.readConfig import *
from tests.aggregationMongoDB.baseTest.P4_inferenceInputTestData import *
from tests.aggregationMongoDB.baseTest.P4_etProcessor import *

def test_P4_045_inferenceInput_censusAccuracy(P4_ETTestClient_EncounterTracking_docs,s3_session):

    inferenceInputProcessor = InferenceInputProcessor(s3_session)
    s3_unitId_censusNum_dict = inferenceInputProcessor.unitId_censusNumber_dict()
    print("S3 dict ", s3_unitId_censusNum_dict)


    etProcessor = ETProcessor(P4_ETTestClient_EncounterTracking_docs)
    mongoDB_unitId_censusNum_dict = etProcessor.unitIdCensusNum_dict()
    print("MongoDB dict", mongoDB_unitId_censusNum_dict)

    for unitId in mongoDB_unitId_censusNum_dict.keys():
        if unitId != "None":
            for localDateTime in mongoDB_unitId_censusNum_dict[unitId].keys():
                assert mongoDB_unitId_censusNum_dict[unitId][localDateTime] == s3_unitId_censusNum_dict[unitId][localDateTime]






