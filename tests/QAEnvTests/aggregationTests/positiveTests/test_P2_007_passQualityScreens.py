import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *
from tests.QAEnvTests.aggregationTests.baseTest.eventIdLst import *
from tests.QAEnvTests.aggregationTests.baseTest.visitNumberLst import *


def test_007_P2():
    filter = MGFilter()
    lister = visitNumberLst()
    visitNumLst = lister.visitNumLst_extractedAdt_pass_PETestClient()
    visitNumTemp = lister.visitNumLst_extractedAdt_pass_temp_PETestClient()
    count_map_1 = {}
    for ele in visitNumLst:
        count = count_map_1.get(ele, None)
        if count is None:
            count = 0

        count_map_1[ele] = count + 1
    print(count_map_1)

    count_map_2 = {}
    patientEn_docs = filter.patientEncounters_PETestClient()
    for doc in patientEn_docs:
        visitNum = doc.get('visitNumber')
        events = doc.get('events')
        # print(len(events))
        count_map_2[visitNum] = len(events)
    print(count_map_2)

    for num in visitNumTemp:
        assert count_map_1.get(num) == count_map_2.get(num)








































