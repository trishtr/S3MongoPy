from tests.QAEnvTests.aggregationTests.visitNumber_eventId_Dict import *
from tests.QAEnvTests.aggregationTests.visitNumberLst import *
from deepdiff import DeepDiff


def test_visitNumber_eventId():
    print('Check matching visitNumber _ eventId list')
    mapper = visitNumberEventIdMapping()
    templst = visitNumberLst()
    visitNum_lst = templst.visitNumTempLst_parsedhl7()
    visitNum_eventId_parsedhl7_map = mapper.visitNum_eventId_parsedhl7()
    visitNum_eventId_extractedAdt_map = mapper.visitNum_eventId_extractedAdt()

    for num in visitNum_lst:
        eventIdLst_extractedAdt = visitNum_eventId_extractedAdt_map.get(num)
        print('extractedAdt _ visitNum, eventId mapping : ', num, eventIdLst_extractedAdt)
        eventIdLst_parsedhl7 = visitNum_eventId_parsedhl7_map.get(num)
        print('parsedhl7 _ visitNum, eventId mapping : ', num, eventIdLst_parsedhl7)

        for eventId in eventIdLst_extractedAdt:
            assert eventId in eventIdLst_parsedhl7









