from tests.aggregationMongoDB.baseTest.P3_visitNum_poc_msgGap_dict_patientEncounter import *
from tests.aggregationMongoDB.baseTest.P4_visitNum_poc_msgGap_dict_encounterTracking import *

def test_P3_038_PE_ET_hourlyRecords(P3_ETTestClient_PatientEncounters_docs,P4_ETTestClient_EncounterTracking_docs):
    PEdict = patientEncounter_dict_creator(P3_ETTestClient_PatientEncounters_docs)
    print("PEdict", PEdict)
    ETdict = encounterTracking_dict_creator(P4_ETTestClient_EncounterTracking_docs)
    print("ETdict", ETdict)
    for visitNumber in PEdict.keys():
        for poc in PEdict[visitNumber].keys():
            if poc != "" and PEdict[visitNumber][poc] >= 1:
                assert abs(PEdict[visitNumber][poc] - ETdict[visitNumber][poc]) < 2

