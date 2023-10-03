

def test_P1_002_visitNumber(P1_EST_HL7_filter,P1_EST_extractedAdt_filter):

    parsed_visitNumberLst = []
    parsed_visitNumber_count = {}
    extracted_visitNumberLst = []
    extracted_visitNumber_count = {}

    docs_parsed = P1_EST_HL7_filter
    for doc in docs_parsed:
        visitNumber =  doc.get("payload").get("eventData").get("ADT").get("fields").get("VisitNumber")
        parsed_visitNumberLst.append(visitNumber)

    for visitNum in parsed_visitNumberLst:
        count = parsed_visitNumber_count.get(visitNum, None)
        if count is None:
            count = 0
        parsed_visitNumber_count[visitNum] = count + 1

    print("visitNumber in parsed HL7 counting map :" , parsed_visitNumber_count)

    docs_extractedAdt = P1_EST_extractedAdt_filter
    for doc in docs_extractedAdt:
        visitNumber =  doc.get("visitNumber")
        extracted_visitNumberLst.append(visitNumber)

    for visitNum in extracted_visitNumberLst:
        count = extracted_visitNumber_count.get(visitNum, None)
        if count is None:
            count = 0
        extracted_visitNumber_count[visitNum] = count + 1

    print("visitNumber in extractedAdtEvents counting map :", extracted_visitNumber_count)

    if extracted_visitNumber_count == parsed_visitNumber_count:
        assert True





