from testBase.QAEnv.filterMGCollection import *
from datetime import *
from tests.QAEnvTests.aggregationTests.baseTest.timeCalculator import *


def test_P3_034_numberDocsInET():
    filter = MGFilter()
    docs = filter.patientEncounters_hasCensusEffect_true_ETTestClient()
    docs_ET = filter.encounterTracking_ETTestClient()

    visitNumberLst = []
    ET_dict = {}
    for doc in docs_ET:
        visitNumber = doc.get('visitNumber')
        visitNumberLst.append(visitNumber)
    # print(visitNumberLst)

    for ele in visitNumberLst:
        count = ET_dict.get(ele, None)
        if count is None:
            count = 0

        ET_dict[ele] = count + 1
    print('Encounter Tracking: ')
    print(ET_dict)

    PE_dict = {}
    now = now_utc().date()
    for doc in docs:
        visitNumber = doc.get('visitNumber')
        admitDateTime = doc.get('admitDateTime')
        admitDateTime_date = doc.get('admitDateTime').date()
        dischargeDateTime = doc.get('dischargeDateTime')

        if dischargeDateTime != None:
            dischargeDateTime_date = dischargeDateTime.date()
            los = dischargeDateTime_date - admitDateTime_date
            PE_dict[visitNumber] = los.days

        else:
            los = now - admitDateTime_date
            PE_dict[visitNumber] = los.days

        compare_time = time(23, 59, 59)

        if (compare_time.hour - admitDateTime.time().hour) > 1:

            ET_dict[visitNumber] == PE_dict[visitNumber] + 1
            print(visitNumber, PE_dict[visitNumber])
        else:
            ET_dict[visitNumber] == PE_dict[visitNumber]
            print(visitNumber, PE_dict[visitNumber])

    print('Patient Encounter')
    print(PE_dict)






