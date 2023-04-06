from testBase.QAEnv.filterMGCollection import *


def test_P2_009_eventId():
    filter = MGFilter()
    docs = filter.extractedAdt_qualityScreens_pass_PETestClient()

    visitNumber_eventId_dict = {}

    for doc in docs:
        eventId = doc.get('eventId')
        visitNumber = doc.get('visitNumber')
        if visitNumber not in visitNumber_eventId_dict:
            visitNumber_eventId_dict[visitNumber] = [eventId]
        else:
            visitNumber_eventId_dict[visitNumber].append(eventId)
    print(visitNumber_eventId_dict)

    docs_EP = filter.patientEncounters_PETestClient()
    for doc in docs_EP:
        visitNumber_EP = doc.get('visitNumber')
        events = doc.get('events')

        for event in events:
            eventId_EP = event.get('eventId')
            print(visitNumber_EP, eventId_EP)
            assert eventId_EP in visitNumber_eventId_dict[visitNumber_EP]



