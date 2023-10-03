from utilities.timeProcessor import *
import datetime

def test_P1_004(P1_EST_extractedAdt_filter, P1_EST_HL7_filter):


    eventIdLst = []
    eventId_timestamp_dict = {}

    messageDateTime_parsed = ''
    messageDateTime_extracted = ''
    messageDateTime_extracted_str = ''
    messageDateTime_parsed_str =''
    messageDateTime_extracted_str_lst = []

    extracted_docs = P1_EST_extractedAdt_filter
    format = '%Y-%m-%d %H:%M:%S'

    print('mapping eventId, messageDateTime_extractedAdt')
    for doc in extracted_docs:
        eventId = doc.get("eventId")
        eventIdLst.append(eventId)

        messageDateTime_extracted = doc.get('messageDateTime')
        messageDateTime_extracted_str = datetime.datetime.strftime(messageDateTime_extracted,format)
        messageDateTime_extracted_str_lst.append(messageDateTime_extracted_str)

        print('eventId : ', eventId, 'messageDateTime : ', messageDateTime_extracted)

    print('mapping eventId- utc converted messageDateTime _ parsedhl7')
    parsed_docs = P1_EST_HL7_filter
    for doc in parsed_docs :
        eventId = doc.get("eventId")
        messageDateTime_parsed_temp = doc.get('payload').get('eventData').get('ADT').get('fields').get('MessageDateTime')
        eventId_timestamp_dict[eventId] = messageDateTime_parsed_temp

        if eventId in eventIdLst:

            messageDateTime_parsed = eventId_timestamp_dict.get(eventId)
            print('messageDateTime before converting to utc : ', messageDateTime_parsed)
            messageDateTime_parsed_str = convert_est_utc(messageDateTime_parsed)

            print(eventId, messageDateTime_parsed_str)
            assert messageDateTime_parsed_str in messageDateTime_extracted_str_lst




