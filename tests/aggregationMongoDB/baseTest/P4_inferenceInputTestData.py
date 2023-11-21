from utilities.readConfig import *
import csv
import io
from tests.aggregationMongoDB.baseTest.P4_objectKeysForTestData import *
from utilities.readConfig import *

class InferenceInputProcessor():
    def __init__(self, s3_session):
        self.session = s3_session


    def unitId_censusNumber_dict(self):
        getter = getObjectKeys(self.session)
        inferenceInputKeys = getter.getInferenceInputObjKeys()
        # Get the keyObj for the latest file
        keyObj = inferenceInputKeys[-1]
        print(keyObj)
        if keyObj is None:
            raise Exception("keyObj is empty")

        s3_client = self.session.client('s3')

        response = s3_client.get_object(Bucket= get_qa_inferenceInputBucket(), Key= keyObj)
        csv_data = response['Body'].read().decode('utf-8')
        csv_lines = csv_data.split('\n')[1:]


        dictFromCsv = {}

        # Process the CSV data
        for line in csv_lines:
            row = line.split(',')
            if len(row) == 3:
                first_column, second_column, third_column = row

                if first_column not in dictFromCsv:
                    dictFromCsv[first_column] = [{second_column: int(third_column)}]
                else:
                    dictFromCsv[first_column].append({second_column:int(third_column)})
        # print(dictFromCsv)

        converted_dict = {}

        for key, value_list in dictFromCsv.items():
            nested_dict = {}
            for item in value_list:
                for inner_key, inner_value in item.items():
                    nested_dict[inner_key] = inner_value
                    converted_dict[key] = nested_dict

        # print(converted_dict)
        return converted_dict

