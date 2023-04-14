from testBase.QAEnv.filterMGCollection import *
from utilities.timeProcessor import *
from zoneinfo import ZoneInfo
from datetime import *


def test_P3_035_localTime_snapshotTime():
    filter = MGFilter()

    docs = filter.encounterTracking_ETTestClient()
    format = '%Y-%m-%d %H:%M:%S'
    for doc in docs:
        visitNumber = doc.get('visitNumber')
        trackings = doc.get('trackingStatus')
        id = doc.get('_id')

        dateUTC = doc.get('dateUTC')
        for track in trackings:
            snapshotTimestamp = track.get('snapshotTimestamp')

            localDateTime = track.get('localDateTime')

            y = snapshotTimestamp.year
            mth = snapshotTimestamp.month
            d = snapshotTimestamp.day
            h = snapshotTimestamp.hour
            mn = snapshotTimestamp.minute
            s = snapshotTimestamp.second

            snapshot_utc = datetime(y, mth, d, h, mn, s, tzinfo=pytz.utc)
            est = pytz.timezone('US/Eastern')

            est_time = snapshot_utc.astimezone(est)
            est_time_str = est_time.strftime(format)

            # print(id, visitNumber, snapshotTimestamp, est_time_str, localDateTime)
            # if visitNumber == 'ETARGVN001' and dateUTC == '2022-11-06':
            #     print(id, visitNumber, snapshotTimestamp, est_time_str, localDateTime )

            assert est_time_str == localDateTime



