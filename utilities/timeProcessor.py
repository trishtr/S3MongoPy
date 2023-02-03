from dateutil.tz import UTC
from dateutil.parser import isoparse
import pytz


def convert_utc_to_est(isoStr):
    fmt = '%Y-%m-%d %H:%M:%S'
    # parser.isoparse convert time String into datetime.datetime
    # to enable to DateTime methods
    time = isoparse(isoStr)

    # time_UTC type : datetime
    time_utc = time.astimezone(UTC)

    # convert to String
    time_utc_str = time_utc.strftime(fmt)
    print('\n')
    print('time in UTC ', time_utc_str)

    # strftime(timeformat) return the timeString
    time_est = time_utc.astimezone(pytz.timezone('US/Eastern')).strftime(fmt)
    print('time in EST ', time_est)

    return time_est






