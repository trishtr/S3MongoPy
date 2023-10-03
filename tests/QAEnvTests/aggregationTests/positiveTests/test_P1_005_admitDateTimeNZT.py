from utilities.timeProcessor import *
import datetime


def test_P1_005(P1_EST_extractedAdt_filter, P1_EST_HL7_filter):


    eventIdLst = []
    eventId_timestamp_dict = {}

    admitDateTime_parsed = ''
    admitDateTime_extracted = ''
    admitDateTime_extracted_str = ''
    admitDateTime_parsed_str =''
    admitDateTime_extracted_str_lst = []

    extracted_docs = P1_EST_extractedAdt_filter
    format = '%Y-%m-%d %H:%M:%S'

    print('mapping eventId, admitDateTime_extractedAdt')
    for doc in extracted_docs:
        eventId = doc.get("eventId")
        eventIdLst.append(eventId)

        admitDateTime_extracted = doc.get('admitDateTime')
        admitDateTime_extracted_str = datetime.datetime.strftime(admitDateTime_extracted,format)
        admitDateTime_extracted_str_lst.append(admitDateTime_extracted_str)

        print(eventId, admitDateTime_extracted)

    print('mapping eventId- utc converted admitDateTime _ parsedhl7')
    parsed_docs = P1_EST_HL7_filter
    for doc in parsed_docs :
        eventId = doc.get("eventId")
        admitDateTime_parsed_temp = doc.get('payload').get('eventData').get('ADT').get('fields').get('AdmitDateTime')
        eventId_timestamp_dict[eventId] = admitDateTime_parsed_temp

        if eventId in eventIdLst:

            admitDateTime_parsed = eventId_timestamp_dict.get(eventId)
            print('admitDateTime before converting to utc : ', admitDateTime_parsed)
            admitDateTime_parsed_str = convert_est_utc(admitDateTime_parsed)

            print(eventId, admitDateTime_parsed_str)
            assert admitDateTime_parsed_str in admitDateTime_extracted_str_lst




