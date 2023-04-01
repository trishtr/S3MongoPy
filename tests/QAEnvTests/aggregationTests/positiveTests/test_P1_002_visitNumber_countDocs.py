from tests.QAEnvTests.aggregationTests.visitNumberLst import *


def test_visitNumber():
    lister = visitNumberLst()

    parsed_hl7Lst = lister.visitNumLst_parsedhl7()
    extracted_AdtLst = lister.visitNumLst_extractAdt()

    count_map_1 = {}
    for ele in parsed_hl7Lst:
        count = count_map_1.get(ele, None)
        if count is None:
            count = 0
        count_map_1[ele] = count + 1

    print('parsedhl7 docs counted based on visitNumber', count_map_1)

    count_map_2 = {}
    for ele in extracted_AdtLst:
        count = count_map_2.get(ele, None)
        if count is None:
            count = 0
        count_map_2[ele] = count + 1

    print('extractedAdtEvent docs counted based on visitNumber', count_map_2)

    if count_map_1 == count_map_2:
        assert True
        print("Docs recorded for each visitNumber in 2 collections are the same")
    else:
        assert False







