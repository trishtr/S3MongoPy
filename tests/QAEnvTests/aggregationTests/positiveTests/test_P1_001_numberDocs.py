import pymongo
import pytest

def test_001_P1(P1_EST_extractedAdt_filter, P1_EST_HL7_filter):


    count_extractedAdt = 0
    count_parsedhl7 = 0

    docs_parsed = P1_EST_HL7_filter
    for doc in docs_parsed:
        count_parsedhl7 += 1
    print('Docs counted in parsedhl7', count_parsedhl7)

    docs_extractedAdt = P1_EST_extractedAdt_filter
    for doc in docs_extractedAdt :
        count_extractedAdt += 1
    print('Docs counted in extractedAdt', count_extractedAdt)


    assert count_extractedAdt == count_parsedhl7


























