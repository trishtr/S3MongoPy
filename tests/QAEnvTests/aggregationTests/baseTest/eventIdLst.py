import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *


class eventIdLst:

    def eventIdLst_patientEncounters(self):
        filter = MGFilter()
        docs = filter.patientEncounters_PETestClient()
        eventIdLst = []
        for doc in docs:
            events = doc.get("events")
            for event in events:
                eventId = event.get("eventId")
                eventIdLst.append(eventId)
        return eventIdLst


