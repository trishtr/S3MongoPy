

def test_P1_003_visitNumber_eventId(P1_EST_HL7_filter,P1_EST_extractedAdt_filter):


    parsed_docs = P1_EST_HL7_filter
    parsed_visitNum_eventId_map = {}
    for doc in  parsed_docs:
        visitNumber =  doc.get("payload").get("eventData").get("ADT").get("fields").get("VisitNumber")
        eventId = doc.get("eventId")

        if visitNumber not in parsed_visitNum_eventId_map:
            parsed_visitNum_eventId_map[visitNumber] = [eventId]
        else:
            parsed_visitNum_eventId_map[visitNumber].append(eventId)
    print("Parsed_visitNum_eventID map : ",  parsed_visitNum_eventId_map)

    extractedAdt_docs = P1_EST_extractedAdt_filter
    extractedAdt_visitNumber_eventId_map = {}
    for doc in extractedAdt_docs:
        visitNumber = doc.get("visitNumber")
        eventId = doc.get("eventId")

        if visitNumber not in extractedAdt_visitNumber_eventId_map:
            extractedAdt_visitNumber_eventId_map[visitNumber] = [eventId]
        else:
            extractedAdt_visitNumber_eventId_map[visitNumber].append(eventId)


        if extractedAdt_visitNumber_eventId_map[visitNumber] == parsed_visitNum_eventId_map[visitNumber]:
            assert True

    print("ExtractedAdt_visitNum_eventID map : ", extractedAdt_visitNumber_eventId_map)