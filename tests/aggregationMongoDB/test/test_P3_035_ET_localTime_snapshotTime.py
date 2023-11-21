from utilities.timeProcessor import *
from zoneinfo import ZoneInfo
from datetime import *

def test_P3_035_ET_localTime_snapshotTime(P4_ETTestClient_EncounterTracking_filterByClientId_docs):


    docs =P4_ETTestClient_EncounterTracking_filterByClientId_docs
    format = '%Y-%m-%d %H:%M:%S'
    for doc in docs :

        trackings = doc.get('trackingStatus')

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

            # assertion : localDateTime will be converted to est tz
            assert est_time_str == localDateTime



