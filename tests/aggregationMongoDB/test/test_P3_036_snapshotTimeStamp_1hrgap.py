from tests.aggregationMongoDB.baseTest.P4_etProcessor import *
from datetime import *

def test_P3_036_localDateTime_1hgap(P4_ETTestClient_EncounterTracking_docs, P4_ETTestClient_EncounterTracking_filterByClientId_docs):
    etProcessor = ETProcessor(P4_ETTestClient_EncounterTracking_docs)
    PE_visitNumber_snapshotTimestamp_dict = etProcessor.visitNum_localDateTime(P4_ETTestClient_EncounterTracking_filterByClientId_docs)

    # print(PE_visitNumber_localDateTime_dict)

    for vn, snapshot in PE_visitNumber_snapshotTimestamp_dict.items():
        snapshot.sort()
        for i in range (len(snapshot)-1):
            gap = snapshot[i+1] - snapshot[i]
            assert gap == timedelta(hours = 1)


