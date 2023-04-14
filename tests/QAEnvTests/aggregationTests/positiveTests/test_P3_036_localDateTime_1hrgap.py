from testBase.QAEnv.filterMGCollection import *
from datetime import *


def test_P3_036_localDateTime_1hgap():
    filter = MGFilter()

    docs = filter.encounterTracking_ETTestClient()

    final_dict = {}
    for doc in docs:
        trackings = doc.get('trackingStatus')
        visitNumber = doc.get('visitNumber')
        for track in trackings:
            snapshotTimestamp = track.get('snapshotTimestamp')

            if visitNumber not in final_dict:
                final_dict[visitNumber] = [snapshotTimestamp]
            else:
                final_dict[visitNumber].append(snapshotTimestamp)

    for vn in final_dict:
        list_timestamp = final_dict[vn]
        for i in range(len(list_timestamp) - 1):
            gap = list_timestamp[i + 1] - list_timestamp[i]

            assert gap == timedelta(hours=1)

