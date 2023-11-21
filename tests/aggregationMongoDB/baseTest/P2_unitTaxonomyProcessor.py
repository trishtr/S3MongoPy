class UTProcessor:
    def __init__(self,P2_PETestClient_unitTaxonomy_docs):
        self.docs = P2_PETestClient_unitTaxonomy_docs
        self.unitId_poc_dict = {}
        self.unitType_customerIds = {}
        self.customerIds_lst= []

    def P2_utaxonomy_unitId_poc_dict(self):
        for doc in self.docs:
            unitId = doc.get('_id')
            pocLst = doc.get("customerIds")
            if unitId not in self.unitId_poc_dict:
                self.unitId_poc_dict[unitId] = pocLst
        return self.unitId_poc_dict

    def P2_utaxonomy_unitType_customerIds_dict(self):
        for doc in self.docs:
            unitType = doc.get("unitType")
            customerIds = doc.get('customerIds')

            if unitType not in self.unitType_customerIds:
                self.unitType_customerIds[unitType] = [customerIds]
            else:
                self.unitType_customerIds[unitType].append(customerIds)
        return self.unitType_customerIds

    def P2_utaxonomy_cutomerIds_lst(self):
        for doc in self.docs:
            customerIds = doc.get('customerIds')

            if customerIds not in self.customerIds_lst:
                self.customerIds_lst.append(customerIds)
        return self.customerIds_lst