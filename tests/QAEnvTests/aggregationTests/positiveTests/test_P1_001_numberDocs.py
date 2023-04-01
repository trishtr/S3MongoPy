import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *


class Test_001_P1:

    def test_countDocs(self):

        filter = MGFilter()

        count_extractedAdt = 0
        count_parsedhl7 = 0
        docs_extractedAdt = filter.extractedAdt_ESTAGGR()
        for doc in docs_extractedAdt:
            count_extractedAdt += 1
        print('Docs counted in extractedAdt', count_extractedAdt)

        docs_parsed = filter.parsedHL7_ESTAGGR()
        for doc in docs_parsed:
            count_parsedhl7 += 1
        print('Docs counted in parsedhl7', count_parsedhl7)

        assert count_extractedAdt == count_parsedhl7



























