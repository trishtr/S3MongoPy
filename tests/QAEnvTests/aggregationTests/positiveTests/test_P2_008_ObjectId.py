from testBase.QAEnv.filterMGCollection import *


def test_P2_008_ObjectId():
    filter = MGFilter()
    docs = filter.extractedAdt_qualityScreens_pass_PETestClient()

    visitNumber_ObjId_dict = {}

    for doc in docs:
        id = doc.get('_id')
        visitNumber = doc.get('visitNumber')
        if visitNumber not in visitNumber_ObjId_dict:
            visitNumber_ObjId_dict[visitNumber] = [id]
        else:
            visitNumber_ObjId_dict[visitNumber].append(id)
    print(visitNumber_ObjId_dict)

    docs_EP = filter.patientEncounters_PETestClient()
    for doc in docs_EP:
        visitNumber_EP = doc.get('visitNumber')
        events = doc.get('events')

        for event in events:
            docId = event.get('docId')
            print(visitNumber_EP, docId)
            assert docId in visitNumber_ObjId_dict[visitNumber_EP]



