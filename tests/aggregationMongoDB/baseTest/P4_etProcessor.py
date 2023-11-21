class ETProcessor():
    def __init__(self, P4_ETTestClient_EncounterTracking_docs):
        self.docs = P4_ETTestClient_EncounterTracking_docs
        self.unitId_censusNumber_dict = {}
        self.visitNumber_countDocs_dict = {}
        self.visitNumber_localDateTime_dict = {}
        self.unitId_poc_dict = {}

    def unitIdCensusNum_dict(self):
        for doc in self.docs:
            trackings = doc.get('trackingStatus')

            for tracking in trackings:

                unitId = str(tracking.get("unitId"))
                localDateTime = tracking.get("localDateTime")
                includeInCensus = tracking.get('includeInCensus')
                if unitId != ""and includeInCensus == True:
                    if unitId not in self.unitId_censusNumber_dict:
                        self.unitId_censusNumber_dict[unitId] = {localDateTime: 1}
                    else:
                        if localDateTime in self.unitId_censusNumber_dict[unitId]:
                            self.unitId_censusNumber_dict[unitId][localDateTime] +=1
                        else:
                            self.unitId_censusNumber_dict[unitId][localDateTime] = 1
        return self.unitId_censusNumber_dict


    def visitNum_countDocs(self,P4_ETTestClient_EncounterTracking_filterByClientId_docs):

        for doc in P4_ETTestClient_EncounterTracking_filterByClientId_docs:
            visitNumber = doc.get('visitNumber')

            if visitNumber not in self.visitNumber_countDocs_dict:
                self.visitNumber_countDocs_dict[visitNumber] =  0
            self.visitNumber_countDocs_dict[visitNumber] += 1

        return self.visitNumber_countDocs_dict

    def visitNum_localDateTime(self,P4_ETTestClient_EncounterTracking_filterByClientId_docs):
        for doc in P4_ETTestClient_EncounterTracking_filterByClientId_docs:
            visitNumber = doc.get('visitNumber')

            trackings = doc.get('trackingStatus')
            for track in trackings:
                snapshotTimestamp = track.get('snapshotTimestamp')
                if visitNumber not in self.visitNumber_localDateTime_dict:
                    self.visitNumber_localDateTime_dict[visitNumber] = [snapshotTimestamp]
                else:
                    self.visitNumber_localDateTime_dict[visitNumber].append(snapshotTimestamp)
        return self.visitNumber_localDateTime_dict

    def unitId_poc(self,P4_ETTestClient_EncounterTracking_filterByClientId_docs):
        for doc in P4_ETTestClient_EncounterTracking_filterByClientId_docs:

            trackings = doc.get('trackingStatus')
            for tracking in trackings:
                unitId = tracking.get('unitId')
                unit = tracking.get('unit')

                if unitId != None or unitId != "" or unitId != 'None':
                    if unitId not in self.unitId_poc_dict:
                        self.unitId_poc_dict[unitId] = [unit]
                    else:
                        if unit not in self.unitId_poc_dict[unitId]:
                            self.unitId_poc_dict[unitId].append(unit)
                        else:
                            self.unitId_poc_dict[unitId] = [unit]
        return self.unitId_poc_dict


