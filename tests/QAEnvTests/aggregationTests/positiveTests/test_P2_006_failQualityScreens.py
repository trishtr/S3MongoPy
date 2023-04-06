from testBase.QAEnv.filterMGCollection import *
from tests.QAEnvTests.aggregationTests.baseTest.eventIdLst import *


def test_006_P2():
    filter = MGFilter()
    lister = eventIdLst()

    count_extractedAdt_fail = 0

    docs_extractedAdt = filter.extractedAdt_qualityScreens_fail_PETestClient()
    for doc in docs_extractedAdt:
        count_extractedAdt_fail += 1
        eventId = doc.get("eventId")
    print('Fail docs counted in extractedAdt', count_extractedAdt_fail)

    eventIdLst_patientEn = lister.eventIdLst_patientEncounters()
    assert eventId not in eventIdLst_patientEn





























