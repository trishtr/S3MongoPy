class UTProcessor:
    def __init__(self,P3_ETTestClient_unitTaxonomy_docs):
        self.docs = P3_ETTestClient_unitTaxonomy_docs
        self.unitId_poc_dict = {}
        self.unitId_lst = []


    def P2_utaxonomy_unitId_poc_dict(self):
        for doc in self.docs:
            unitId = doc.get('_id')
            pocLst = doc.get("customerIds")
            if unitId not in self.unitId_poc_dict:
                self.unitId_poc_dict[unitId] = pocLst
        return self.unitId_poc_dict

    def S3_utaxonomy_unitId_list(self):
        for doc in self.docs:
            unitId = str(doc.get('_id'))

            if unitId not in self.unitId_lst:
                self.unitId_lst.append(unitId)
        return self.unitId_lst