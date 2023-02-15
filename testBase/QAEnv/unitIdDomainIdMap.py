import pymongo

from testBase.QAEnv.filterMGCollection import MGFilter


class unitDomainMap:
    filter = MGFilter()
    docs = filter.unitTaxonomyFilterTestClient()

    def unit_domainId_map(self):

        for doc in self.docs:

            id = doc.get('_id')
            # print(id)
            # print(type(id))

            for internalId in doc.get('internalIds'):
                domainId = internalId.get('domainId')
                # print(domainId)
                unit_domainId_map = {}
                unit_domainId_map[domainId] = id
            print(unit_domainId_map)



