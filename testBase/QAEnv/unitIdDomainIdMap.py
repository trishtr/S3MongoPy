import pymongo

from testBase.QAEnv.filterMGCollection import MGFilter


class unitDomainMap:
    filter = MGFilter()
    docs = filter.unitTaxonomyFilterTestClient()
    unitIdMap = {}

    def unit_domainId_map(self):

        for doc in self.docs:

            id = doc.get('_id')

            for internalId in doc.get('internalIds'):
                domainId = internalId.get('domainId')

            self.unitIdMap[domainId] = id
        # print(self.unitIdMap)
        return self.unitIdMap







