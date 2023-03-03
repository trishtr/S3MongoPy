from utilities.timeProcessor import *
from testBase.QAEnv.filterMGCollection import *


def test_func():
    filter = MGFilter()

    eventIdLst = []
    eventId_timestamp_dict = {}

    messageDateTime_parsed = ''
    messageDateTime_extracted = ''
    messageDateTime_extracted_str = ''
    messageDateTime_parsed_str = ''
    messageDateTime_extracted_str_lst = []

    extracted_docs = filter.extractedAdt_ESTAGGR_false_WIP()
    format = '%Y-%m-%d %H:%M:%S'

    print('mapping eventId, messageDateTime_extractedAdt')
    for doc in extracted_docs:
        eventId = doc.get("eventId")
        eventIdLst.append(eventId)

        messageDateTime_extracted = doc.get('messageDateTime')
        messageDateTime_extracted_str = datetime.datetime.strftime(messageDateTime_extracted, format)
        messageDateTime_extracted_str_lst.append(messageDateTime_extracted_str)

        print(eventId, messageDateTime_extracted)

    print('mapping eventId- utc converted messageDateTime _ parsedhl7')
    parsed_docs = filter.parsedHL7_ESTAGGR()
    for doc in parsed_docs:
        eventId = doc.get("eventId")
        messageDateTime_parsed_temp = doc.get('payload').get('eventData').get('ADT').get('fields').get(
            'MessageDateTime')
        eventId_timestamp_dict[eventId] = messageDateTime_parsed_temp

        if eventId in eventIdLst:
            messageDateTime_parsed = eventId_timestamp_dict.get(eventId)
            print('messageDateTime before converting to utc : ', messageDateTime_parsed)
            messageDateTime_parsed_str = convert_est_utc(messageDateTime_parsed)

            print(eventId, messageDateTime_parsed_str)
            assert messageDateTime_parsed_str in messageDateTime_extracted_str_lst




