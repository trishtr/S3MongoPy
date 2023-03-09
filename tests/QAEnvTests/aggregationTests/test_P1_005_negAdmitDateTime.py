from utilities.timeProcessor import *
from testBase.QAEnv.filterMGCollection import *
from dateutil.parser import isoparse
from dateutil import parser


def test_P1_005_invalidMessageDateTime():
    filter = MGFilter()

    extracted_docs = filter.extractedAdt_ESTNEGAGR()

    for doc in extracted_docs:
        eventId = doc.get("eventId")

        admitDateTime_extracted = doc.get('admitDateTime')

        if isinstance(admitDateTime_extracted, str):
            try:
                datetime.datetime.fromisoformat(admitDateTime_extracted)
            except:
                print(eventId)
                print(admitDateTime_extracted)
                assert True
            else:
                assert False












