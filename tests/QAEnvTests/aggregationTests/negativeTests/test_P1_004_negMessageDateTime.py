from utilities.timeProcessor import *
from testBase.QAEnv.filterMGCollection import *
from dateutil.parser import isoparse
from dateutil import parser


def test_P1_004_negMessageDateTime():
    filter = MGFilter()

    extracted_docs = filter.extractedAdt_ESTNEGAGR()

    for doc in extracted_docs:
        eventId = doc.get("eventId")

        messageDateTime_extracted = doc.get('messageDateTime')

        if isinstance(messageDateTime_extracted, str):
            try:
                datetime.datetime.fromisoformat(messageDateTime_extracted)
            except:
                print(eventId)
                print(messageDateTime_extracted)
                assert True
            else:
                assert False












