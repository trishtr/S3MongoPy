from testBase.QAEnv.filterMGCollection import *


def encounterTracking_dict_creator():
    filter = MGFilter()

    unit_count = {}
    final_dict = {}
    docs = filter.encounterTracking_PETestClient()

    for doc in docs:
        visitNumber = doc.get('visitNumber')
        tracking = doc.get('trackingStatus')

        for track in tracking:

            unit = track.get('unit')

            if visitNumber not in unit_count:
                unit_count[visitNumber] = [unit]
            else:
                unit_count[visitNumber].append(unit)

    for v in unit_count.keys():
        count = {x: unit_count[v].count(x) for x in unit_count[v]}
        final_dict[v] = count

    print('Encounter Tracking: ')
    print(final_dict)

    return final_dict





