from tests.QAEnvTests.aggregationTests.baseTest.timeCalculator import *
from testBase.QAEnv.filterMGCollection import *
from tests.QAEnvTests.aggregationTests.baseTest.visitNum_poc_msgGap_dict_encounterTracking import *
from tests.QAEnvTests.aggregationTests.baseTest.visitNum_poc_msgGap_dict_patientEncounter import *


def test_P3_038_timegap():
    PE_dict = patientEncounter_dict_creator()

    ET_dict = encounterTracking_dict_creator()

    filter = MGFilter()

    docs = filter.encounterTracking_PETestClient()

    for doc in docs:
        visitNumber = doc.get('visitNumber')
        trackings = doc.get('trackingStatus')

        for track in trackings:
            unit = track.get('unit')

            assert PE_dict[visitNumber][unit] - ET_dict[visitNumber][unit] < 1





