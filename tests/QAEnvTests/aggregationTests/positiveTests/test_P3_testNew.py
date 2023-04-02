from testBase.QAEnv.filterMGCollection import *
from tests.QAEnvTests.aggregationTests.baseTest.visitNum_poc_msgGap_dict_encounterTracking import *
from datetime import *


def test_2():
    filter = MGFilter()

    test_dict = {
        "visitNumber": "abc",
        "dischargeDateTime": 11.0,
        "events": [
            {
                "pointOfCare": "a1",
                "messageDateTime": 1
            },
            {
                "pointOfCare": "a2",
                "messageDateTime": 3
            },
            {
                "pointOfCare": "a1",
                "messageDateTime": 7
            },
            {
                "pointOfCare": "a2",
                "messageDateTime": 9
            }
        ]
    }

    visitByPatient = {}
    events = test_dict["events"]
    visitNumber = test_dict["visitNumber"]
    for i in range(len(events)):
        visitId = events[i]["pointOfCare"]
        if i == len(events) - 1:
            if "dischargeDateTime" not in test_dict:
                # end_date = now()
                end_date = 11111
            else:
                end_date = test_dict["dischargeDateTime"]

            duration = end_date - events[i]["messageDateTime"]
        else:
            duration = events[i + 1]["messageDateTime"] - events[i]["messageDateTime"]

        if visitNumber not in visitByPatient:
            visitByPatient[visitNumber] = {}

        storedDuration = visitByPatient[visitNumber].get(visitId, 0)
        storedDuration += duration
        visitByPatient[visitNumber][visitId] = storedDuration

        print(f"{events[i]['pointOfCare']} : {duration}")

    print("Final Result: ")
    print(visitByPatient)