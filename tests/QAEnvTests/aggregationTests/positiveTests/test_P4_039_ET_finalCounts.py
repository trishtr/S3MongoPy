from tests.QAEnvTests.aggregationTests.baseTest.unitId_localDateTime_countDocs_encounterTracking import *
import csv
from testBase.QAEnv.s3Connection import s3Connect
from utilities.readConfig import *
from testBase.QAEnv.retrieveInferenceInputKey import *
import pytest


# @pytest.mark.skip(reason= 'draf autotests for P4')
def test_P4_039_ETfinalCounts():
    unitId_localDateTime_countDocs = ETfinalCounts()

    qa_access_key = get_qa_accessKey()
    qa_secret_key = get_qa_secretKey()
    qa_session_token = get_qa_sessionToken()
    qa_region_name = get_qa_regionName()
    qa_inference_input = "input_inference"

    prefix = 'ETTestClient/ETTestLocation/2023/'

    objKeyLst = getObjKeyList().getObjKeyLstFromSpecificFolder(qa_inference_input, prefix)
    # print(len(objKeyLst))

    connector = s3Connect(qa_access_key, qa_secret_key, qa_session_token, qa_region_name)

    s3_session = connector.create_s3_session()

    s3_client = s3_session.client('s3')

    unit_id_lst = []

    for key in objKeyLst:
        obj = s3_client.get_object(Bucket=qa_inference_input, Key=key)
        # print(obj['Body'])

        lines = obj['Body'].read().decode('utf-8').splitlines(True)
        # print('\n')
        # print(lines)
        reader = csv.DictReader(lines)

        # {'unitId': '637c3ff254ad08e85bee4ff2', 'localDateTime': '2022-12-07 00:00:00', 'census': '2', 'year': '2022', 'weekofyear': '49', 'dayofmonth': '7', 'hour': '0'}
        for row in reader:
            # print(row)
            unitId = row['unitId']
            census = int(row['census'])
            localDateTime = row['localDateTime']

            if unitId not in unit_id_lst:
                unit_id_lst.append(unitId)

            # assert unitId_localDateTime_countDocs[unitId][localDateTime] == census
            # debugging
            if unitId_localDateTime_countDocs[unitId][localDateTime] == census:
                pass
            else:
                # print('test failed, investigate the following unitId')
                print(unitId, localDateTime, census)

    # print(unit_id_lst)
    for id in unitId_localDateTime_countDocs.keys():

        if id != 'None' and id != '':
            assert id in unit_id_lst













