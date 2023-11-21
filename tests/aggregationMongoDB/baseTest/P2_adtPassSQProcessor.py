class ADTProcessor():

    def __init__(self,P2_PETestClient_extractedAdt_passQualityScreens_docs):

        self.adt_docs = P2_PETestClient_extractedAdt_passQualityScreens_docs

        self.adt_visitNum_lst = []
        self.P2_adt_visitNumber_docId_dict = {}
        self.P2_adt_visitNumber_eventId_dict = {}
        self.P2_adt_visitNumber_patientTypes_dict = {}
        self.P2_adt_visitNumber_admissionTypes_dict = {}
        self.P2_adt_visitNumber_patientClass_dict = {}
        self.P2_adt_visitNumber_admitDateTime_dict = {}
        self.P2_adt_visitNumber_messageType_dict = {}
        self.P2_adt_visitNumber_eventTypeCode_dict = {}
        self.P2_adt_visitNumber_messageDateTime_dict = {}
        self.P2_adt_visitNumber_pointOfCare_dict = {}
        self.P2_adt_admitDateTime_messageDT_dict = {}
        self.P2_adt_unitId_POC_dict = {}
        self.P2_adt_visitNumber_unitId_dict = {}
        self.P2_adt_visitNumber_unitType_dict = {}
        self.P2_adt_unitType_POC_dict = {}
        self.P2_adt_visitNumber_assignedFacility_dict = {}
        self.P2_adt_visitNumber_hospitalService_dict = {}
        self.P2_adt_visitNumber_dischargeDateTime_dict = {}
        self.P2_adt_patClass_includeInCensus_dict = {}


    def P2_adt_visitNumber_SQ_pass_notunique_lst(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            self.adt_visitNum_lst.append(visitNumber)
        return self.adt_visitNum_lst


    def P2_adt_visitNum_docId_dict(self):
        for doc in self.adt_docs :
            id = doc.get('_id')
            visitNumber = doc.get('visitNumber')

            # adt VisitNumber:[docId]
            if visitNumber not in self.P2_adt_visitNumber_docId_dict:
                self.P2_adt_visitNumber_docId_dict[visitNumber] = [id]
            else:
                self.P2_adt_visitNumber_docId_dict[visitNumber].append(id)
        return self.P2_adt_visitNumber_docId_dict

    def P2_adt_visitNum_eventId_dict(self):

        for doc in self.adt_docs :
            eventId = doc.get('eventId')
            visitNumber = doc.get('visitNumber')

            # adt visitNumber:[eventId]
            if visitNumber not in self.P2_adt_visitNumber_eventId_dict:
                self.P2_adt_visitNumber_eventId_dict[visitNumber] = [eventId]
            else:
                self.P2_adt_visitNumber_eventId_dict[visitNumber].append(eventId)
        return self.P2_adt_visitNumber_eventId_dict

    def P2_adt_visitNum_patientTypes_dict(self):
        for doc in self.adt_docs :
            patientType = doc.get('patientType')

            visitNumber = doc.get('visitNumber')
            if visitNumber not in self.P2_adt_visitNumber_patientTypes_dict:
                self.P2_adt_visitNumber_patientTypes_dict[visitNumber] = [patientType]
            else :
                self.P2_adt_visitNumber_patientTypes_dict[visitNumber].append(patientType)
        return self.P2_adt_visitNumber_patientTypes_dict

    def P2_adt_visitNum_admissionType_dict(self):

        for doc in self.adt_docs :
            visitNumber = doc.get('visitNumber')
            admissionType = doc.get('admissionType')

            if visitNumber not in self.P2_adt_visitNumber_admissionTypes_dict:
                self.P2_adt_visitNumber_admissionTypes_dict[visitNumber] = [admissionType]
            else:
                self.P2_adt_visitNumber_admissionTypes_dict[visitNumber].append(admissionType)
        return self.P2_adt_visitNumber_admissionTypes_dict

    def P2_adt_visitNum_patientClass_dict(self):

        for doc in self.adt_docs :
            visitNumber = doc.get('visitNumber')
            patientClass = doc.get('patientClass')

            if visitNumber not in self.P2_adt_visitNumber_patientClass_dict:
                self.P2_adt_visitNumber_patientClass_dict[visitNumber] = [patientClass]
            else:
                self.P2_adt_visitNumber_patientClass_dict[visitNumber].append(patientClass)
        return self.P2_adt_visitNumber_patientClass_dict

        # TODO: add logic for the senarios : one visitNumber having multiple admitDateTime in ADT
    # PE will pick up the LATEST.
    def P2_adt_visitNum_admitDateTime_dict(self,P2_PETestClient_extractedAdt_passQualityScreens_includeInCensus_True_docs):

        for doc in P2_PETestClient_extractedAdt_passQualityScreens_includeInCensus_True_docs:
            visitNumber = doc.get('visitNumber',)
            admitDateTime = doc.get('admitDateTime')

            if visitNumber not in self.P2_adt_visitNumber_admitDateTime_dict:
                self.P2_adt_visitNumber_admitDateTime_dict[visitNumber] = [admitDateTime]
            elif admitDateTime not in self.P2_adt_visitNumber_admitDateTime_dict[visitNumber]:
                self.P2_adt_visitNumber_admitDateTime_dict[visitNumber].append(admitDateTime)

        return self.P2_adt_visitNumber_admitDateTime_dict

    def P2_adt_visitNum_messageType_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            messageType = doc.get('messageType')

            if visitNumber not in self.P2_adt_visitNumber_messageType_dict:
                self.P2_adt_visitNumber_messageType_dict[visitNumber] = [messageType]
            else:
                self.P2_adt_visitNumber_messageType_dict[visitNumber].append(messageType)
        return self.P2_adt_visitNumber_messageType_dict

    def P2_adt_visitNum_eventTypeCode_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            eventTypeCode = doc.get('eventTypeCode')

            if visitNumber not in self.P2_adt_visitNumber_eventTypeCode_dict:
                self.P2_adt_visitNumber_eventTypeCode_dict[visitNumber] = [eventTypeCode]
            else:
                self.P2_adt_visitNumber_eventTypeCode_dict[visitNumber].append(eventTypeCode)
        return self.P2_adt_visitNumber_eventTypeCode_dict

    def P2_adt_visitNum_messageDateTime_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            messageDateTime = doc.get('messageDateTime')

            if visitNumber not in self.P2_adt_visitNumber_messageDateTime_dict:
                self.P2_adt_visitNumber_messageDateTime_dict[visitNumber] = [messageDateTime]
            else:
                self.P2_adt_visitNumber_messageDateTime_dict[visitNumber].append(messageDateTime)
        return self.P2_adt_visitNumber_messageDateTime_dict


    def P2_adt_visitNum_pointOfCare_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            pointOfCare = doc.get('pointOfCare')

            if visitNumber not in self.P2_adt_visitNumber_pointOfCare_dict:
                self.P2_adt_visitNumber_pointOfCare_dict[visitNumber] = [pointOfCare]
            else:
                self.P2_adt_visitNumber_pointOfCare_dict[visitNumber].append(pointOfCare)
        return self.P2_adt_visitNumber_pointOfCare_dict

    def P2_adt_admitDT_messageDT_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            admitDT = doc.get('admitDateTime')
            messageDT = doc.get('messageDateTime')

            if visitNumber in self.P2_adt_admitDateTime_messageDT_dict:
                if admitDT in self.P2_adt_admitDateTime_messageDT_dict[visitNumber]:
                    self.P2_adt_admitDateTime_messageDT_dict[visitNumber][admitDT] = max(self.P2_adt_admitDateTime_messageDT_dict[visitNumber][admitDT], messageDT)
                else:
                    # If not, create a new list with the messageDT
                    self.P2_adt_admitDateTime_messageDT_dict[visitNumber][admitDT] = max([messageDT])
            else:
                # If visitNumber is not in the dictionary, create a new sub-dictionary
                self.P2_adt_admitDateTime_messageDT_dict[visitNumber] = {admitDT: max([messageDT])}

        return self.P2_adt_admitDateTime_messageDT_dict


    def P2_adt_unitId_poc_dict(self):
        for doc in self.adt_docs:
            pointOfCare = doc.get("pointOfCare")
            unitId = doc.get('unitId')
            if unitId not in  self.P2_adt_unitId_POC_dict:
                self.P2_adt_unitId_POC_dict[unitId] = set()

            self.P2_adt_unitId_POC_dict[unitId].add(pointOfCare)
        return self.P2_adt_unitId_POC_dict

    def P2_adt_visitNum_unitId_dict(self):
        for doc in self.adt_docs:
            unitId = doc.get('unitId')
            visitNumber = doc.get('visitNumber')

            if unitId is not None:
                if visitNumber not in self.P2_adt_visitNumber_unitId_dict:
                    self.P2_adt_visitNumber_unitId_dict[visitNumber] = [unitId]
                else:
                    self.P2_adt_visitNumber_unitId_dict[visitNumber].append(unitId)
        return self.P2_adt_visitNumber_unitId_dict

    def P2_adt_visitNum_unitType_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            unitType = doc.get("unitType")

            if unitType is not None:
                notNoneUnitType = unitType[:5]
                if visitNumber not in self.P2_adt_visitNumber_unitType_dict:
                    self.P2_adt_visitNumber_unitType_dict[visitNumber] = [notNoneUnitType]
                else:
                    self.P2_adt_visitNumber_unitType_dict[visitNumber].append(notNoneUnitType)
        return self.P2_adt_visitNumber_unitType_dict

    def P2_adt_unitType_poc_dict(self):
        for doc in self.adt_docs:
            pointOfCare = doc.get("pointOfCare")
            unitType = doc.get('unitType')
            if unitType not in  self.P2_adt_unitType_POC_dict:
                self.P2_adt_unitType_POC_dict[unitType] = set()

            self.P2_adt_unitType_POC_dict[unitType].add(pointOfCare)
        return self.P2_adt_unitType_POC_dict

    def P2_adt_visitNum_assignedFacility_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            assignedFacility = doc.get('assignedFacility')


            if visitNumber not in self.P2_adt_visitNumber_assignedFacility_dict:
                self.P2_adt_visitNumber_assignedFacility_dict[visitNumber] = [assignedFacility]
            else:
                self.P2_adt_visitNumber_assignedFacility_dict[visitNumber].append(assignedFacility)
        return self.P2_adt_visitNumber_assignedFacility_dict

    def P2_adt_visitNum_hospitalService_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            hospitalService = doc.get('hospitalService')

            if visitNumber not in self.P2_adt_visitNumber_hospitalService_dict:
                self.P2_adt_visitNumber_hospitalService_dict[visitNumber] = [hospitalService]
            else:
                self.P2_adt_visitNumber_hospitalService_dict[visitNumber].append(hospitalService)
        return self.P2_adt_visitNumber_hospitalService_dict

    def P2_adt_visitNum_dischargeDateTime_dict(self):
        for doc in self.adt_docs:
            visitNumber = doc.get('visitNumber')
            dischargeDateTime = doc.get('dischargeDateTime')

            if dischargeDateTime is not None:
                if visitNumber not in self.P2_adt_visitNumber_dischargeDateTime_dict:

                    self.P2_adt_visitNumber_dischargeDateTime_dict[visitNumber] = [dischargeDateTime]
                else:
                    self.P2_adt_visitNumber_dischargeDateTime_dict[visitNumber].append(dischargeDateTime)
        return self.P2_adt_visitNumber_dischargeDateTime_dict

    # def P2_adt_patientClass_includeInCensus_dict(self):
    #     for doc in self.docs:
    #         patientClass = doc.get("patientClass")
    #         includeInCensus = doc.get('includeInCensus')

    #         if patientClass not in self.P2_adt_patClass_includeInCensus_dict():


    # def visitNumber_extractedAdt_quality_pass_includeInCenus_true_TestClient(self):
    #     filter = MGFilter()
    #     docs = filter.extractedAdt_screenquality_pass_includeInCensus_true_TestClient()

    #     visitNumLst = []
    #     for doc in docs :
    #         visitNum = doc.get('visitNumber')
    #         if visitNum not in visitNumLst :
    #             visitNumLst.append(visitNum)
    #     return visitNumLst



