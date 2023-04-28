from testBase.QAEnv.filterMGCollection import *


def ETfinalCounts():
    filter = MGFilter()
    docs = filter.encounterTracking_includeInCensus_true_ETTestClient()

    final_dict = {}

    for doc in docs:
        tracks = doc.get('trackingStatus')

        for track in tracks:
            unitId = track.get('unitId')
            if type(unitId) != str:
                unitId = str(unitId)

            localDateTime = track.get('localDateTime')
            if unitId not in final_dict:
                final_dict[unitId] = [localDateTime]
            else:
                final_dict[unitId].append(localDateTime)

    for id in final_dict.keys():
        count = {x: final_dict[id].count(x) for x in final_dict[id]}
        final_dict[id] = count

    print(final_dict)
    return final_dict


