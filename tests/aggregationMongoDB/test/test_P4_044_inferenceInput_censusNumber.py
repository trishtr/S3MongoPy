from utilities.readConfig import *
from tests.aggregationMongoDB.baseTest.P4_readCensusByHour import *
from datetime import datetime
import re

def test_P4_044_inferenceInput_census(s3_session):

    csv_lines =  inferenceInputCsv(s3_session)

    for line in csv_lines[1:]:
        row = line.split(',')
        # print(row)
        if row != ['']:
            census = row[2].strip()
            if census == None or census == '' or census == 0:
                raise Exception ('census could not be None, null or empty')

            # print(census)
            # assert cenusNumber as whole int
            assert int(census) * 10 % 10 == 0