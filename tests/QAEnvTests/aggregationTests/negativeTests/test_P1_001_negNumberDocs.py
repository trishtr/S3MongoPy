import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *


def test_001_P1_neg():
    filter = MGFilter()

    count_extractedAdt = 0
    count_parsedhl7 = 0
    docs_extractedAdt = filter.extractedAdt_ESTNEGAGR()
    for doc in docs_extractedAdt:
        count_extractedAdt += 1
    print('Docs counted in extractedAdt', count_extractedAdt)

    docs_parsed = filter.parsedHL7_ESTNEGAGR()
    for doc in docs_parsed:
        count_parsedhl7 += 1
    print('Docs counted in parsedhl7', count_parsedhl7)

    assert count_extractedAdt == count_parsedhl7



























