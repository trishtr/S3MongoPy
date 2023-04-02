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

    def visitNumLst_extractedAdt_pass_PETestClient(self):
        filter = MGFilter()
        docs = filter.extractedAdt_qualityScreens_pass_PETestClient()

        visitNumLst = []
        for doc in docs:
            visitNum = doc.get('visitNumber')
            visitNumLst.append(visitNum)
        return visitNumLst

    def visitNumLst_extractedAdt_pass_temp_PETestClient(self):
        filter = MGFilter()
        docs = filter.extractedAdt_qualityScreens_pass_PETestClient()

        visitNumLst = []
        for doc in docs:
            visitNum = doc.get('visitNumber')
            if visitNum not in visitNumLst:
                visitNumLst.append(visitNum)
        return visitNumLst

    def visitNumber_extractedAdt_quality_pass_includeInCenus_true_TestClient(self):
        filter = MGFilter()
        docs = filter.extractedAdt_screenquality_pass_includeInCensus_true_TestClient()

        visitNumLst = []
        for doc in docs:
            visitNum = doc.get('visitNumber')
            if visitNum not in visitNumLst:
                visitNumLst.append(visitNum)
        return visitNumLst



