from datetime import *


def now_substract_time(time):
    now_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
    naive_now = now_utc.replace(tzinfo=None)
    # print(naive_now)
    gap = (naive_now.timestamp() - time.timestamp())/(60*60)
    # print(gap)
    return gap

def timeend_substract_timestart(timestart, timeend):
    gap = (timeend.timestamp() - timestart.timestamp())/(60*60)
    # print(gap)
    return gap

def now_utc_epoch():
    now_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
    naive_now = now_utc.replace(tzinfo=None)
    naive_now_epoch = naive_now.timestamp()
    return naive_now_epoch

def now_utc():
    now_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
    naive_now = now_utc.replace(tzinfo=None)
    return naive_now