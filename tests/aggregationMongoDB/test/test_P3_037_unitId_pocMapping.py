from tests.aggregationMongoDB.baseTest.P4_unitTaxonomyProcessor import *
from tests.aggregationMongoDB.baseTest.P4_etProcessor import *

def test_P3_037_unitId_pocMapping(P3_ETTestClient_unitTaxonomy_docs,P4_ETTestClient_EncounterTracking_docs,P4_ETTestClient_EncounterTracking_filterByClientId_docs):

    utProcessor = UTProcessor(P3_ETTestClient_unitTaxonomy_docs)
    UT_unitId_poc_map = utProcessor.P2_utaxonomy_unitId_poc_dict()

    etProcessor = ETProcessor(P4_ETTestClient_EncounterTracking_docs)
    ET_unitId_poc_map = etProcessor.unitId_poc(P4_ETTestClient_EncounterTracking_filterByClientId_docs)
    print(ET_unitId_poc_map)

    # for unitId, poc in ET_unitId_poc_map.items():
    #     if (unitId is not None and poc is not None) and (unitId!= '' and poc != ''):
    #             assert poc in UT_unitId_poc_map[unitId]





