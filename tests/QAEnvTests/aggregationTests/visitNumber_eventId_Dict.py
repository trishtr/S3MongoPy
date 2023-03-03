import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *


class visitNumberEventIdMapping:

    def visitNum_eventId_parsedhl7(self):
        filter = MGFilter()
        docs = filter.parsedHL7_ESTAGGR()

        visitNum_eventId_parsedhl7_dict = {}
        for doc in docs:
            visitNum = doc.get("payload").get("eventData").get("ADT").get("fields").get("VisitNumber")
            eventId = doc.get("eventId")
            if visitNum not in visitNum_eventId_parsedhl7_dict:
                visitNum_eventId_parsedhl7_dict[visitNum] = [eventId]
            else:
                visitNum_eventId_parsedhl7_dict[visitNum].append(eventId)
        # print(visitNum_eventId_parsedhl7_dict)
        return visitNum_eventId_parsedhl7_dict

    def visitNum_eventId_extractedAdt(self):
        filter = MGFilter()
        docs = filter.extractedAdt_ESTAGGR_false_WIP()

        visitNum_eventId_extractedAdt_dict = {}
        for doc in docs:
            visitNum = doc.get("visitNumber")
            eventId = doc.get("eventId")
            if visitNum not in visitNum_eventId_extractedAdt_dict:
                visitNum_eventId_extractedAdt_dict[visitNum] = [eventId]
            else:
                visitNum_eventId_extractedAdt_dict[visitNum].append(eventId)
        # print(visitNum_eventId_extractedAdt_dict)
        return visitNum_eventId_extractedAdt_dict