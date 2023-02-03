from utilities.timeProcessor import convert_utc_to_est


def test_time_001():
    convert_utc_to_est('2023-03-23T10:00:00.000-04:00')
    convert_utc_to_est('2023-05-19T07:00:00.000-04:00')






