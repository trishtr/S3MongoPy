import pymongo
import pytest
from testBase.QAEnv.filterMGCollection import *


class visitNumberLst:

    def visitNumTempLst_parsedhl7(self):
        filter = MGFilter()
        docs = filter.parsedHL7_ESTAGGR()
        tempLst = []

        for doc in docs:
            visitNum = doc.get("payload").get("eventData").get("ADT").get("fields").get("VisitNumber")
            if (visitNum not in tempLst):
                tempLst.append(visitNum)
        return tempLst

    def visitNumLst_parsedhl7(self):
        filter = MGFilter()
        docs = filter.parsedHL7_ESTAGGR()

        visitNumLst = []
        for doc in docs:
            visitNum = doc.get("payload").get("eventData").get("ADT").get("fields").get("VisitNumber")
            visitNumLst.append(visitNum)
        return visitNumLst

    def visitNumLst_extractAdt(self):
        filter = MGFilter()
        docs = filter.extractedAdt_ESTAGGR_false_WIP()

        visitNumLst = []
        for doc in docs:
            visitNum = doc.get("visitNumber")
            visitNumLst.append(visitNum)
        return visitNumLst





