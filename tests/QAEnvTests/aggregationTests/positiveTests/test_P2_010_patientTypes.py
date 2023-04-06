from testBase.QAEnv.filterMGCollection import *


def test_P2_010_patientType_1():
    filter = MGFilter()

    visitNum_patientType_extractedAdt = {}
    docs = filter.extractedAdt_qualityScreens_pass_PETestClient()
    for doc in docs:
        patientType = doc.get('patientType')

        visitNumber = doc.get('visitNumber')
        if visitNumber not in visitNum_patientType_extractedAdt:
            visitNum_patientType_extractedAdt[visitNumber] = [patientType]
        else:
            visitNum_patientType_extractedAdt[visitNumber].append(patientType)

    print(visitNum_patientType_extractedAdt)

    docs_patientEncounters = filter.patientEncounters_PETestClient()
    for doc in docs_patientEncounters:
        visitNum_patientEnc = doc.get('visitNumber')
        patientTypes = doc.get("patientTypes")
        print(visitNum_patientEnc, patientTypes)
        assert visitNum_patientType_extractedAdt[visitNum_patientEnc].sort() == patientTypes.sort()



