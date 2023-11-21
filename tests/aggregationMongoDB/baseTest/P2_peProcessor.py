class PEProcessor():
    def __init__(self,P2_PETestClient_PatientEncounters_docs):

        self.docs = P2_PETestClient_PatientEncounters_docs
        self.PE_visitNum_docId_dict = {}
        self.PE_visitNum_eventId_dict = {}
        self.PE_visitNum_patientTypes_dict = {}
        self.PE_visitNum_patientType_dict = {}
        self.PE_visitNum_admissionTypes_dict = {}
        self.PE_visitNum_patientClasses_dict = {}
        self.PE_visitNum_patientClass_dict = {}
        self.PE_visitNum_admitDateTime_dict = {}
        self.PE_visitNum_messageType_dict = {}
        self.PE_visitNum_eventTypeList_dict = {}
        self.PE_visitNum_eventTypeCode_dict = {}
        self.PE_visitNum_messageDateTime_dict = {}
        self.PE_visitNum_pointOfCareList_dict = {}
        self.PE_visitNum_pointOfCare_dict = {}
        self.PE_visitNum_minMDT_dict = {}
        self.PE_visitNum_maxMDT_dict = {}
        self.PE_visitNum_unitId_dict = {}
        self.PE_visitNum_unitTypeList_dict = {}
        self.PE_visitNum_unitType_dict = {}
        self.PE_visitNum_assignedFacility_dict = {}
        self.PE_visitNum_hospitalService_dict = {}
        self.PE_visitNumber_dischargeDateTime_dict = {}


    def P2_pe_visitNum_docId_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                docId = event.get('docId')

                # pe : visitNumber:[docId]
                if visitNumber_PE not in self.PE_visitNum_docId_dict:
                    self.PE_visitNum_docId_dict[visitNumber_PE] = [docId]
                else:
                    self.PE_visitNum_docId_dict[visitNumber_PE].append(docId)
        return self.PE_visitNum_docId_dict


    def P2_pe_visitNum_eventId_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                eventId = event.get('eventId')

                if visitNumber_PE not in self.PE_visitNum_eventId_dict:
                    self.PE_visitNum_eventId_dict[visitNumber_PE] = [eventId]
                else:
                    self.PE_visitNum_eventId_dict[visitNumber_PE].append(eventId)
        return self.PE_visitNum_eventId_dict

    def P2_visitNum_patientTypes_dict(self):
        for doc in self.docs:
            visitNum_PE = doc.get('visitNumber')
            patientTypes = doc.get("patientTypes")
            if visitNum_PE not in self.PE_visitNum_patientTypes_dict:
                self.PE_visitNum_patientTypes_dict[visitNum_PE] = patientTypes
        return self.PE_visitNum_patientTypes_dict

    def P2_visitNum_patientType_dict(self):
        for doc in self.docs:
            visitNum_PE = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                patientType = event.get('patientType')
                if visitNum_PE not in self.PE_visitNum_patientType_dict:
                    self.PE_visitNum_patientType_dict[visitNum_PE] = [patientType]
                else:
                    self.PE_visitNum_patientType_dict[visitNum_PE].append(patientType)
        return self.PE_visitNum_patientType_dict

    def P2_visitNum_admissionTypes_dict(self):
        for doc in self.docs:
            visitNum_PE = doc.get('visitNumber')
            admissionTypes = doc.get("admissionTypes")
            if visitNum_PE not in self.PE_visitNum_patientTypes_dict:
                self.PE_visitNum_patientTypes_dict[visitNum_PE] = admissionTypes
        return self.PE_visitNum_patientTypes_dict

    def P2_visitNum_patientClasses_dict(self):
        for doc in self.docs:
            visitNum_PE = doc.get('visitNumber')
            patientClasses = doc.get("patientClasses")
            if visitNum_PE not in self.PE_visitNum_patientClasses_dict:
                self.PE_visitNum_patientClasses_dict[visitNum_PE] = patientClasses
        return self.PE_visitNum_patientClasses_dict


    def P2_visitNum_patientClass_dict(self):
        for doc in self.docs:
            events = doc.get("events")
            visitNum_PE = doc.get('visitNumber')
            for event in events:
                patientClass = event.get('patientClass')

                if visitNum_PE not in self.PE_visitNum_patientClass_dict:
                    self.PE_visitNum_patientClass_dict[visitNum_PE] = [patientClass]
                else:
                    self.PE_visitNum_patientClass_dict[visitNum_PE].append(patientClass)
        return self.PE_visitNum_patientClass_dict

    def P2_visitNum_admitDateTime_dict(self):
        for doc in self.docs:
            visitNum = doc.get('visitNumber')
            admitDateTime = doc.get('admitDateTime')
            self.PE_visitNum_admitDateTime_dict[visitNum] = [admitDateTime]
        return self.PE_visitNum_admitDateTime_dict


    def P2_pe_visitNum_messageType_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                messageType = event.get('messageType')

                if visitNumber_PE not in self.PE_visitNum_messageType_dict:
                    self.PE_visitNum_messageType_dict[visitNumber_PE] = [messageType]
                else:
                    self.PE_visitNum_messageType_dict[visitNumber_PE].append(messageType)
        return self.PE_visitNum_messageType_dict

    def P2_pe_visitNum_eventTypeList_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')

            eventTypeLst = list(doc.get('eventTypeList').split(','))[:-1]

            if visitNumber_PE not in self.PE_visitNum_eventTypeList_dict:
                self.PE_visitNum_eventTypeList_dict[visitNumber_PE] = eventTypeLst

        return self.PE_visitNum_eventTypeList_dict

    def P2_pe_visitNum_eventTypeCode_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                eventTypeCode = event.get('eventTypeCode')

                if visitNumber_PE not in self.PE_visitNum_eventTypeCode_dict:
                    self.PE_visitNum_eventTypeCode_dict[visitNumber_PE] = [eventTypeCode]
                else:
                    self.PE_visitNum_eventTypeCode_dict[visitNumber_PE].append(eventTypeCode)
        return self.PE_visitNum_eventTypeCode_dict

    def P2_pe_visitNum_minMDT_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            minMDT = doc.get('keyDateTimes').get('minMessageDateTime')

            if visitNumber_PE not in self.PE_visitNum_minMDT_dict:
                self.PE_visitNum_minMDT_dict[visitNumber_PE] = minMDT

        return self.PE_visitNum_minMDT_dict

    def P2_pe_visitNum_maxMDT_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            maxMDT = doc.get('keyDateTimes').get('maxMessageDateTime')

            if visitNumber_PE not in self.PE_visitNum_maxMDT_dict:
                self.PE_visitNum_maxMDT_dict[visitNumber_PE] = maxMDT

        return self.PE_visitNum_maxMDT_dict

    def P2_pe_visitNum_messageDateTime_dict(self):
        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                messageDateTime = event.get('messageDateTime')

                if visitNumber_PE not in self.PE_visitNum_messageDateTime_dict:
                    self.PE_visitNum_messageDateTime_dict[visitNumber_PE] = [messageDateTime]
                else:
                    self.PE_visitNum_messageDateTime_dict[visitNumber_PE].append(messageDateTime)
        return self.PE_visitNum_messageDateTime_dict

    def P2_pe_visitNum_pointOfCareList_dict(self):
        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            pointOfCareList = list(doc.get('pointOfCareList').split(","))[:-1]

            if visitNumber_PE not in self.PE_visitNum_pointOfCareList_dict:
                self.PE_visitNum_pointOfCareList_dict[visitNumber_PE] = pointOfCareList

        return self.PE_visitNum_pointOfCareList_dict

    def P2_pe_visitNum_pointOfCare_dict(self):
        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get("events")
            for event in events :
                pointOfCare = event.get("pointOfCare")

                if visitNumber_PE not in self.PE_visitNum_pointOfCare_dict:
                    self.PE_visitNum_pointOfCare_dict[visitNumber_PE] = [pointOfCare]
                else:
                    self.PE_visitNum_pointOfCare_dict[visitNumber_PE].append(pointOfCare)
        return self.PE_visitNum_pointOfCare_dict

    def P2_pe_visitNum_unitId_dict(self):
        for doc in self.docs:
            events = doc.get("events")
            visitNum_PE = doc.get('visitNumber')
            for event in events:
                unitId = event.get('unitId')

                if unitId is not None:
                    if visitNum_PE not in self.PE_visitNum_unitId_dict:
                        self.PE_visitNum_unitId_dict[visitNum_PE] = [unitId]
                    else:
                        self.PE_visitNum_unitId_dict[visitNum_PE].append(unitId)
        return self.PE_visitNum_unitId_dict


    def P2_pe_visitNum_unitTypeList_dict(self):
        for doc in self.docs:
            visitNumber = doc.get('visitNumber')
            unitTypeList = list(doc.get('unitTypeList').split(','))[:-1]

            if visitNumber not in self.PE_visitNum_unitTypeList_dict:
                self.PE_visitNum_unitTypeList_dict[visitNumber] = unitTypeList
        return self.PE_visitNum_unitTypeList_dict

    def P2_pe_visitNum_unitType_dict(self):
        for doc in self.docs:
            visitNumber = doc.get('visitNumber')
            events = doc.get('events')
            for event in events:
                unitType = event.get('unitType')

                if visitNumber not in self.PE_visitNum_unitType_dict:
                    self.PE_visitNum_unitType_dict[visitNumber] = [unitType]
                else:
                    self.PE_visitNum_unitType_dict[visitNumber].append(unitType)
        return self.PE_visitNum_unitType_dict

    def P2_pe_visitNum_assignedFacility_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                assignedFacility = event.get('assignedFacility')

                if visitNumber_PE not in self.PE_visitNum_assignedFacility_dict:
                    self.PE_visitNum_assignedFacility_dict[visitNumber_PE] = [assignedFacility]
                else:
                    self.PE_visitNum_assignedFacility_dict[visitNumber_PE].append(assignedFacility)
        return self.PE_visitNum_assignedFacility_dict

    def P2_pe_visitNum_hospitalService_dict(self):

        for doc in self.docs:
            visitNumber_PE  = doc.get('visitNumber')
            events = doc.get('events')

            for event in events:
                hospitalService = event.get('hospitalService')

                if visitNumber_PE not in self.PE_visitNum_hospitalService_dict:
                    self.PE_visitNum_hospitalService_dict[visitNumber_PE] = [hospitalService]
                else:
                    self.PE_visitNum_hospitalService_dict[visitNumber_PE].append(hospitalService)
        return self.PE_visitNum_hospitalService_dict

    def P2_pe_visitNum_dischargeDateTime_dict(self):
        for doc in self.docs:
            visitNumber = doc.get('visitNumber')
            dischargeDateTime = doc.get('dischargeDateTime')

            if dischargeDateTime is not None:
                if visitNumber not in self.PE_visitNumber_dischargeDateTime_dict:
                    self.PE_visitNumber_dischargeDateTime_dict[visitNumber] = [dischargeDateTime]
                else:
                    self.PE_visitNumber_dischargeDateTime_dict[visitNumber].append(dischargeDateTime)
        return self.PE_visitNumber_dischargeDateTime_dict