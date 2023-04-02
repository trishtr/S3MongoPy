from testBase.QAEnv.filterMGCollection import *
from tests.QAEnvTests.aggregationTests.baseTest.visitNum_poc_msgGap_dict_encounterTracking import *
from datetime import *
from tests.QAEnvTests.aggregationTests.baseTest.timeCalculator import *


def patientEncounter_dict_creator():
    filter = MGFilter()

    docs = filter.patientEncounters_hasCensusEffect_true_PETestClient()

    visitByPatient = {}
    for doc in docs:
        visitNumber = doc.get('visitNumber')
        dischargeDateTime = doc.get('dischargeDateTime')
        admitDateTime = doc.get('admitDateTime')
        raw_events = doc.get('events')
        events = []
        # events = doc.get('events')

        for i in range(len(raw_events)):
            if dischargeDateTime != None:

                if not (raw_events[i].get('messageDateTime') < (admitDateTime - timedelta(hours=1)) or raw_events[
                    i].get('messageDateTime') > (dischargeDateTime)):
                    events.append(raw_events[i])

            else:
                if not raw_events[i].get('messageDateTime') < (admitDateTime - timedelta(hours=1)):
                    events.append(raw_events[i])

        # print(events)
        # print(len(events))

        for i in range(len(events)):
            visitId = events[i]["pointOfCare"]

            if len(events) == 1:
                if dischargeDateTime != None:
                    # dischargeDateTime - events[0].get('messageDateTime')
                    duration = timeend_substract_timestart(events[0].get('messageDateTime'), dischargeDateTime, )
                else:
                    # now - events[0].get('messageDateTime')
                    duration = now_substract_time(events[0].get('messageDateTime'))

                if visitNumber not in visitByPatient:
                    visitByPatient[visitNumber] = {
                        events[0].get('pointOfCare'): duration
                    }
                else:
                    visitByPatient[visitNumber][events[0].get('pointOfCare')] = duration

                print(visitByPatient)
                return visitByPatient

            if i == len(events) - 1:
                if dischargeDateTime == None:

                    end_date = now_utc_epoch()
                else:
                    end_date = dischargeDateTime.timestamp()

                duration = (end_date - events[i]["messageDateTime"].timestamp()) / (60 * 60)

            else:

                # events[i+1]["messageDateTime"] - events[i]["messageDateTime"]
                duration = (events[i + 1]["messageDateTime"].timestamp() - events[i]["messageDateTime"].timestamp()) / (
                            60 * 60)

            if visitNumber not in visitByPatient:
                visitByPatient[visitNumber] = {}

            storedDuration = visitByPatient[visitNumber].get(visitId, 0)
            storedDuration += duration
            visitByPatient[visitNumber][visitId] = storedDuration

            # print(f"{events[i]['pointOfCare']} : {duration}")

    print("Patient Encounter : ")

    print(visitByPatient)
    return visitByPatient


