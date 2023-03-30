from testBase.QAEnv.filterMGCollection import *
from datetime import *


def test_P3_structureMessageDateTime():
    filter = MGFilter()

    start_date_lst = []

    pointOfCare_lst = []
    docs_PE = filter.patientEncounters_dischargeDateTime_notNull_hasCensussEffect_true_PETestClient()
    for doc in docs_PE:
        admitDateTime = doc.get('admitDateTime')
        dischargeDateTime = doc.get('dischargeDateTime')
        minMessageDateTime = doc.get('minMessageDateTime')
        maxMessageDateTime = doc.get('maxMessageDateTime')
        events = doc.get('events')
        visitNumber = doc.get('visitNumber')
        start_date_lst.append(admitDateTime)

        for event in events:
            messageDateTime = event.get('messageDateTime')
            pointOfCare = event.get('pointOfCare')

            start_date_lst.append(messageDateTime)
            pointOfCare_lst.append(pointOfCare)

        if dischargeDateTime != None:
            start_date_lst.append(dischargeDateTime)

        else:
            start_date_lst.insert(datetime.datetime.now())
        print(start_date_lst, len(start_date_lst))

        for date in start_date_lst:
            if date == minMessageDateTime < admitDateTime:
                # print(date.timestamp())
                start_date_lst.remove(date)
            if date == maxMessageDateTime > dischargeDateTime:
                start_date_lst.remove(date)
        print(start_date_lst, len(start_date_lst))

        date_convert_epoch_lst = []
        for date in start_date_lst:
            date_to_epoch = date.timestamp()
            date_convert_epoch_lst.append(date_to_epoch)

    intervalList = [y - x for x, y in zip(date_convert_epoch_lst, date_convert_epoch_lst[1:])]
    for interval in intervalList:
        epoch_to_hr = interval / (60 * 60)
        print(epoch_to_hr)

    print(visitNumber)
    # print(start_date_lst, len(start_date_lst))
    print(pointOfCare_lst, len(pointOfCare_lst))