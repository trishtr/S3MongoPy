from tests.aggregationMongoDB.baseTest.timeCalculator import *

class PEProcessor():
    def __init__(self,P3_ETTestClient_PatientEncounters_docs):
        self.docs = P3_ETTestClient_PatientEncounters_docs
        self.visitNumber_los_dict = {}

    def visitNum_los_dict(self):
        for doc in self.docs:
            visitNumber = doc.get('visitNumber')
            keyDateTimes = doc.get('keyDateTimes')
            emergencyAdmitDateTime = keyDateTimes.get('emergencyAdmitDateTime')
            inpatientAdmitDateTime = keyDateTimes.get('inpatientAdmitDateTime')
            emergencyDischargeDateTime = keyDateTimes.get('emergencyDischargeDateTime')
            inpatientDischargeDateTime = keyDateTimes.get('inpatientDischargeDateTime')

            countedAdmitDateTime = ''
            countedDischargeDateTime = ''
            los = 0


            if emergencyAdmitDateTime == None and emergencyDischargeDateTime == None and inpatientDischargeDateTime != None:
                countedAdmitDateTime = inpatientAdmitDateTime
                countedDischargeDateTime = inpatientDischargeDateTime
                los = timeend_substract_timestart(countedAdmitDateTime,countedDischargeDateTime)

            if emergencyAdmitDateTime != None:
                countedAdmitDateTime = min(emergencyAdmitDateTime,inpatientAdmitDateTime)
                countedDischargeDateTime = inpatientDischargeDateTime
                los = timeend_substract_timestart(countedAdmitDateTime, countedDischargeDateTime)

            if inpatientAdmitDateTime == None:
                countedAdmitDateTime = emergencyAdmitDateTime
                countedDischargeDateTime = emergencyDischargeDateTime
                los = timeend_substract_timestart(countedAdmitDateTime, countedDischargeDateTime)

            if emergencyDischargeDateTime == None and inpatientDischargeDateTime == None and emergencyDischargeDateTime == None:
                countedAdmitDateTime = inpatientAdmitDateTime
                los = now_substract_time(countedAdmitDateTime)

            if emergencyDischargeDateTime == None and inpatientDischargeDateTime == None and inpatientAdmitDateTime == None:
                countedAdmitDateTime = emergencyAdmitDateTime
                los = now_substract_time(countedAdmitDateTime)

            if emergencyAdmitDateTime == None and emergencyDischargeDateTime == None:
                los = timeend_substract_timestart(inpatientAdmitDateTime,inpatientDischargeDateTime)
            elif inpatientAdmitDateTime == None and inpatientDischargeDateTime == None:
                los = timeend_substract_timestart(emergencyAdmitDateTime, emergencyDischargeDateTime)

            if visitNumber not in self.visitNumber_los_dict:
                self.visitNumber_los_dict[visitNumber] = los/24
        return self.visitNumber_los_dict