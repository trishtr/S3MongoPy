from testBase.QAEnv.filterMGCollection import *
import datetime


def test_sample():
    filter = MGFilter()

    docs = filter.patientEncounters_hasCensusEffect_true_PETestClient()

    result = {}

    for doc in docs:
        visitNumber = doc.get('visitNumber')
        dischargeDateTime = doc.get('dischargeDateTime')
        admitDateTime = doc.get('admitDateTime')
        raw_events = doc.get('events')
        events = []
        # events = doc.get('events')

        for i in range(len(raw_events) - 1):
            if dischargeDateTime != 0:
                if not raw_events[i].get('messageDateTime') < admitDateTime or raw_events[i].get(
                        'messageDateTime') > dischargeDateTime:
                    events.append(raw_events[i])
            else:
                if not events[i].get('messageDateTime') < admitDateTime:
                    events.append(raw_events[i])

        print(events)
        print(len(events))

        for i in range((len(events)) - 1):
            if i == len(events) - 1:
                break
            event = events[i]
            resultByPatient = result.get(visitNumber, {})
            resultByPatient[event.get('pointOfCare')] = events[i + 1].get('messageDateTime') - events[i].get(
                'messageDateTime')
            result[visitNumber] = resultByPatient
        last_event = events[len(events) - 1]

        if dischargeDateTime != None:
            tmp_date = dischargeDateTime - last_event.get('messageDateTime')
        else:
            tmp_date = datetime.datetime.now() - last_event.get('messageDateTime')

        if visitNumber not in result:
            result[visitNumber] = {
                last_event.get('pointOfCare'): tmp_date
            }
        else:
            result[visitNumber][last_event.get('pointOfCare')] = tmp_date

    print(result)


